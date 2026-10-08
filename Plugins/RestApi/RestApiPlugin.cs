using Ape.Core.Determinism;
using Ape.Core.Event;
using Ape.Core.Logging;
using Ape.Core.Replication;
using Ape.Core.Runtime.Plugin;
using Ape.Core.Scene;
using Ape.Core.Scene.Commit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Numerics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Ape.Module.RestApi.Plugin.RestApi;

/// <summary>
/// REST API Plugin - Exposes SceneGraph via HTTP endpoints using HttpListener.
/// Lives under <c>Ape.Module.RestApi/Plugins/RestApi</c> (assembly <c>Ape.Module.RestApi.Plugin.RestApi</c>).
/// 
/// Endpoints:
///   GET    /api/v1/scene              → Full scene hierarchy
///   GET    /api/v1/scene/*            → Get node by path
///   POST   /api/v1/scene/*            → Create node
///   PUT    /api/v1/scene/*            → Update node properties (full replacement)
///   PATCH  /api/v1/scene/*            → Property batch update (partial)
///   GET    /api/v1/scene/entities/                    → All registered entities
///   GET    /api/v1/scene/entities/{prefix}/           → Entities under id prefix (trailing slash)
///   GET    /api/v1/scene/entities/{id}                → Single entity by full id
///   WS     /api/v1/scene/stream         → Scene event stream (subscribe by path)
/// 
/// Configuration (JSON):
/// {
///   "enabled": true,
///   "host": "0.0.0.0",
///   "port": 9090,
///   "corsOrigins": ["*"]
/// }
///
/// <c>host</c>: <c>"*"</c> / <c>"+"</c> / <c>"0.0.0.0"</c> listens on every NIC
/// (LAN, VPN, ZeroTier). A specific address or hostname binds only that one.
/// HTTP and WebSocket share the same listener.
/// </summary>
public class RestApiPlugin : IPlugin, IDeterministicFrameParticipant
{
    public string PluginId => "rest-api";
    public string Name => "REST API Plugin";

    public string ParticipantId => PluginId;
    public FramePhase Phase => FramePhase.Publish;
    public int Order => 50;

    private ISceneRead? _sceneRead;
    private IEventManager? _eventManager;
    private ILogger? _logger;
    private HttpListener? _listener;
    private Task? _serverTask;
    private CancellationTokenSource? _cts;
    private SceneStreamHub? _streamHub;
    private IServiceProvider? _services;
    private readonly IngressBuffer<ISceneCommitRequest> _pendingWrites = new(IngressOrdering.CaptureOrder);
    
    private string _host = "0.0.0.0"; // Listen on all interfaces, including VPN.
    private int _port = 9090; // Default port
    private string[] _corsOrigins = new[] { "*" }; // Default: allow all
    
    private static readonly JsonSerializerOptions _jsonOptions = SceneEntitySerializer.Options;

    public void OnInit(IServiceProvider services)
    {
        _services = services;
        _sceneRead = services.GetService(typeof(ISceneRead)) as ISceneRead;
        _eventManager = services.GetService(typeof(IEventManager)) as IEventManager;
        _logger = services.GetService(typeof(ILogger)) as ILogger;

        if (_sceneRead != null)
            _streamHub = new SceneStreamHub(_sceneRead, _logger);

        (services.GetService(typeof(IFrameParticipantRegistry)) as IFrameParticipantRegistry)?.Register(this);
        _eventManager?.Subscribe<PropertyChangedEvent>(PluginId, OnPropertyChanged);
        _logger?.LogInfo($"[{Name}] Initialized");
    }

    public void OnHostFrame(in FrameContext context, IFrameCommitBatch commits)
    {
        foreach (var item in _pendingWrites.DrainOrdered())
            commits.Enqueue(item.Payload);
    }

    public void OnRun(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        
        // Load configuration
        LoadConfig();

        _logger?.LogInfo($"[RestAPI] Starting HTTP server on {_host}:{_port}...");

        try
        {
            _listener = StartHttpListener(_host, _port);
            LogListenEndpoints();

            // Start request handling loop
            _serverTask = Task.Run(() => HandleRequestsAsync(_cts.Token), _cts.Token);

            // Drain PropertyChanged → WebSocket stream (events are queued per plugin).
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    _eventManager?.DrainEventsFor(PluginId);
                    Task.Delay(20, _cts.Token).Wait(_cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] Failed to start server: {ex.Message}");
        }
    }

    public void OnShutdown()
    {
        _logger?.LogInfo($"[{Name}] Shutting down...");
        _eventManager?.Unsubscribe<PropertyChangedEvent>(PluginId);
        
        try
        {
            // First, cancel the token to stop accepting new requests
            _cts?.Cancel();
            
            // Then stop and close the listener
            _listener?.Stop();
            _listener?.Close();
            
            // Wait a bit for the server task to finish
            _serverTask?.Wait(TimeSpan.FromSeconds(1));
        }
        catch (Exception ex)
        {
            _logger?.LogWarning($"[{Name}] Shutdown error (ignored): {ex.Message}");
        }
        
        _logger?.LogInfo($"[{Name}] Stopped");
    }

    private void LoadConfig()
    {
        var opt = _services != null ? RestApiConfigReader.GetPluginSection(_services) : null;
        if (opt != null)
        {
            _host = opt.GetString("host", _host);
            _port = opt.GetInt("port", _port);
            var cors = opt.GetStringArray("corsOrigins");
            if (cors.Count > 0)
                _corsOrigins = cors.ToArray();
            _logger?.LogInfo($"[RestAPI] Loaded host config: host={_host}, port={_port}");
            return;
        }

        // Legacy: plugins/rest-api.json next to host cwd
        var configPath = Path.Combine(Directory.GetCurrentDirectory(), "plugins", "rest-api.json");
        
        if (!File.Exists(configPath))
        {
            _logger?.LogWarning($"[RestAPI] Config not found at {configPath}, using defaults (host: {_host}, port: {_port})");
            return;
        }

        try
        {
            var json = File.ReadAllText(configPath);
            var config = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json);
            
            if (config != null)
            {
                if (config.TryGetValue("host", out var hostElement) && hostElement.ValueKind == JsonValueKind.String)
                {
                    var host = hostElement.GetString();
                    if (!string.IsNullOrWhiteSpace(host))
                        _host = host;
                }

                if (config.TryGetValue("port", out var portElement))
                    _port = portElement.GetInt32();
                
                if (config.TryGetValue("corsOrigins", out var corsElement))
                {
                    var origins = corsElement.EnumerateArray().Select(e => e.GetString() ?? "*").ToArray();
                    if (origins.Length > 0)
                        _corsOrigins = origins;
                }

                _logger?.LogInfo($"[RestAPI] Loaded config: host={_host}, port={_port}, corsOrigins={string.Join(", ", _corsOrigins)}");
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] Error loading config: {ex.Message}");
        }
    }

    private HttpListener StartHttpListener(string host, int port)
    {
        var listener = CreateListener(BuildPrefixes(host, port));
        try
        {
            listener.Start();
            return listener;
        }
        catch (HttpListenerException ex) when (IsWildcardHost(host))
        {
            listener.Close();
            _logger?.LogWarning(
                $"[RestAPI] Wildcard bind failed ({ex.Message}); falling back to per-interface prefixes");

            listener = CreateListener(BuildInterfacePrefixes(port));
            listener.Start();
            return listener;
        }
    }

    private static HttpListener CreateListener(IEnumerable<string> prefixes)
    {
        var listener = new HttpListener();
        foreach (var prefix in prefixes)
            listener.Prefixes.Add(prefix);
        return listener;
    }

    private static bool IsWildcardHost(string host) =>
        string.IsNullOrWhiteSpace(host) ||
        host is "*" or "+" or "0.0.0.0" or "::" or "[::]";

    private static IReadOnlyList<string> BuildPrefixes(string host, int port)
    {
        if (IsWildcardHost(host))
            return new[] { $"http://+:{port}/" };

        return new[] { $"http://{FormatPrefixHost(host)}:{port}/" };
    }

    private static IReadOnlyList<string> BuildInterfacePrefixes(int port)
    {
        var prefixes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            $"http://127.0.0.1:{port}/",
            $"http://localhost:{port}/",
        };

        foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (ni.OperationalStatus != OperationalStatus.Up)
                continue;

            foreach (var unicast in ni.GetIPProperties().UnicastAddresses)
            {
                var address = unicast.Address;
                if (address.AddressFamily == AddressFamily.InterNetwork)
                    prefixes.Add($"http://{address}:{port}/");
                else if (address.AddressFamily == AddressFamily.InterNetworkV6 &&
                         !address.IsIPv6LinkLocal &&
                         !address.IsIPv6Multicast)
                    prefixes.Add($"http://[{address}]:{port}/");
            }
        }

        return prefixes.ToList();
    }

    private static string FormatPrefixHost(string host)
    {
        host = host.Trim();
        if (host.StartsWith('[') && host.EndsWith(']'))
            host = host[1..^1];

        if (IPAddress.TryParse(host, out var ip) && ip.AddressFamily == AddressFamily.InterNetworkV6)
            return $"[{ip}]";
        return host;
    }

    private void LogListenEndpoints()
    {
        _logger?.LogInfo($"[RestAPI] Listening on host={_host} port={_port} (HTTP + WebSocket)");
        foreach (var address in EnumerateAdvertisedAddresses(_host))
        {
            _logger?.LogInfo($"[RestAPI] ✓ HTTP  http://{address}:{_port}/api/v1/scene");
            _logger?.LogInfo($"[RestAPI] ✓ WS    ws://{address}:{_port}/api/v1/scene/stream");
        }
    }

    private static IEnumerable<string> EnumerateAdvertisedAddresses(string host)
    {
        if (!IsWildcardHost(host))
        {
            yield return FormatPrefixHost(host);
            yield break;
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "127.0.0.1", "localhost" };
        foreach (var value in seen)
            yield return value;

        foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (ni.OperationalStatus != OperationalStatus.Up)
                continue;

            foreach (var unicast in ni.GetIPProperties().UnicastAddresses)
            {
                var address = unicast.Address;
                if (address.AddressFamily != AddressFamily.InterNetwork)
                    continue;
                if (IPAddress.IsLoopback(address))
                    continue;
                if (!seen.Add(address.ToString()))
                    continue;
                yield return address.ToString();
            }
        }
    }

    private async Task HandleRequestsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _listener != null && _listener.IsListening)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                _ = Task.Run(() => ProcessRequestAsync(context, cancellationToken), cancellationToken);
            }
            catch (HttpListenerException)
            {
                // Listener stopped - this is normal during shutdown
                break;
            }
            catch (ObjectDisposedException)
            {
                // Listener disposed - this is normal during shutdown
                break;
            }
            catch (Exception ex)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    _logger?.LogError($"[RestAPI] Error accepting request: {ex.Message}");
                }
                break;
            }
        }
    }

    private async Task ProcessRequestAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        try
        {
            var request = context.Request;
            var response = context.Response;

            // CORS headers
            if (_corsOrigins.Contains("*"))
            {
                response.Headers.Add("Access-Control-Allow-Origin", "*");
            }
            else if (request.Headers["Origin"] != null && _corsOrigins.Contains(request.Headers["Origin"]))
            {
                response.Headers.Add("Access-Control-Allow-Origin", request.Headers["Origin"]);
            }
            
            response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
            response.Headers.Add("Access-Control-Allow-Headers", "Content-Type");

            // Handle OPTIONS preflight
            if (request.HttpMethod == "OPTIONS")
            {
                response.StatusCode = 204;
                response.Close();
                return;
            }

            var path = request.Url?.AbsolutePath ?? "/";
            var method = request.HttpMethod;

            _logger?.LogDebug($"[RestAPI] {method} {path}");

            if (path == "/api/v1/scene/stream" && request.IsWebSocketRequest)
            {
                if (_streamHub == null)
                {
                    response.StatusCode = 503;
                    await WriteJsonResponse(response, new { error = "SCENE_UNAVAILABLE" });
                    return;
                }

                var wsContext = await context.AcceptWebSocketAsync(subProtocol: null).ConfigureAwait(false);
                await _streamHub.HandleClientAsync(wsContext.WebSocket, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Route to handlers
            if (path.StartsWith("/api/v1/scene"))
            {
                var scenePath = path.Substring("/api/v1/scene".Length);
                if (string.IsNullOrEmpty(scenePath)) scenePath = "/";

                await HandleSceneRequest(method, scenePath, request, response);
            }
            else
            {
                // 404 for other paths
                response.StatusCode = 404;
                await WriteJsonResponse(response, new { error = "NOT_FOUND", message = "Endpoint not found" });
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] Error processing request: {ex.Message}");
            try
            {
                context.Response.StatusCode = 500;
                await WriteJsonResponse(context.Response, new { error = "INTERNAL_ERROR", message = ex.Message });
            }
            catch { }
        }
    }

    private void OnPropertyChanged(PropertyChangedEvent evt) =>
        _streamHub?.OnPropertyChanged(evt);

    private async Task HandleSceneRequest(string method, string scenePath, HttpListenerRequest request, HttpListenerResponse response)
    {
        if (scenePath.StartsWith("/entities", StringComparison.Ordinal))
        {
            await HandleEntityRequest(method, scenePath, response).ConfigureAwait(false);
            return;
        }

        switch (method)
        {
            case "GET":
                await HandleGetScene(scenePath, response);
                break;
            
            case "POST":
                await HandleCreateNode(scenePath, request, response);
                break;
            
            case "PUT":
            case "PATCH":
                await HandleUpdateNode(scenePath, request, response);
                break;
            
            case "DELETE":
                await HandleDeleteNode(scenePath, response);
                break;
            
            default:
                response.StatusCode = 405;
                await WriteJsonResponse(response, new { error = "METHOD_NOT_ALLOWED", message = $"Method {method} not allowed" });
                break;
        }
    }

    private async Task HandleEntityRequest(string method, string scenePath, HttpListenerResponse response)
    {
        if (_sceneRead == null)
        {
            response.StatusCode = 503;
            await WriteJsonResponse(response, new { error = "SCENE_UNAVAILABLE" });
            return;
        }

        if (method != "GET")
        {
            response.StatusCode = 405;
            await WriteJsonResponse(response, new { error = "METHOD_NOT_ALLOWED" });
            return;
        }

        var entityPath = scenePath["/entities".Length..];
        if (string.IsNullOrEmpty(entityPath) || entityPath == "/")
        {
            await WriteEntityListResponse(response, _sceneRead.GetAllEntities(), prefix: null);
            return;
        }

        var relativePath = entityPath.TrimStart('/');
        if (SceneEntityPathResolver.IsPrefixCollection(relativePath))
        {
            var prefix = SceneEntityPathResolver.NormalizePrefix(relativePath);
            var entities = _sceneRead.GetAllEntities()
                .Where(e => SceneEntityPathResolver.MatchesPrefix(((IReplica)e).Id, prefix));
            await WriteEntityListResponse(response, entities, prefix);
            return;
        }

        var entity = _sceneRead.GetEntity(relativePath);
        if (entity == null)
        {
            response.StatusCode = 404;
            await WriteJsonResponse(response, new
            {
                error = "NOT_FOUND",
                message = $"Entity not found: {relativePath}",
                id = relativePath,
            });
            return;
        }

        response.StatusCode = 200;
        await WriteJsonResponse(response, SceneEntitySerializer.ToDto(entity));
    }

    private async Task WriteEntityListResponse(
        HttpListenerResponse response,
        IEnumerable<IEntity> entities,
        string? prefix)
    {
        var list = entities.Select(SceneEntitySerializer.ToDto).ToList();
        response.StatusCode = 200;
        await WriteJsonResponse(response, new
        {
            entities = list,
            totalCount = list.Count,
            prefix,
            timestamp = DateTime.UtcNow,
        });
    }

    private async Task HandleGetScene(string scenePath, HttpListenerResponse response)
    {
        try
        {
            if (scenePath == "/" || scenePath == "")
            {
                // Get all nodes
                var allNodes = (_sceneRead?.GetAllNodes() ?? Enumerable.Empty<INode>()).ToList();
                response.StatusCode = 200;
                await WriteJsonResponse(response, new 
                { 
                    nodes = allNodes,
                    totalCount = allNodes.Count,
                    timestamp = DateTime.UtcNow
                });
            }
            else
            {
                // Get specific node
                if (!scenePath.StartsWith("/"))
                    scenePath = "/" + scenePath;

                var node = _sceneRead?.GetNodeByPath(scenePath);
                if (node == null)
                {
                    response.StatusCode = 404;
                    await WriteJsonResponse(response, new 
                    { 
                        error = "NOT_FOUND",
                        message = $"Node not found at path: {scenePath}",
                        path = scenePath
                    });
                }
                else
                {
                    response.StatusCode = 200;
                    await WriteJsonResponse(response, node);
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] HandleGetScene error: {ex.Message}");
            response.StatusCode = 500;
            await WriteJsonResponse(response, new { error = "INTERNAL_ERROR", message = ex.Message });
        }
    }

    private async Task HandleCreateNode(string scenePath, HttpListenerRequest request, HttpListenerResponse response)
    {
        try
        {
            response.StatusCode = 501;
            await WriteJsonResponse(response, new { error = "NOT_IMPLEMENTED", message = "Node creation not yet implemented" });
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] HandleCreateNode error: {ex.Message}");
            response.StatusCode = 500;
            await WriteJsonResponse(response, new { error = "INTERNAL_ERROR", message = ex.Message });
        }
    }

    private async Task HandleUpdateNode(string scenePath, HttpListenerRequest request, HttpListenerResponse response)
    {
        try
        {
            if (!scenePath.StartsWith("/"))
                scenePath = "/" + scenePath;

            var node = _sceneRead?.GetNodeByPath(scenePath);
            if (node == null)
            {
                response.StatusCode = 404;
                await WriteJsonResponse(response, new 
                { 
                    error = "NOT_FOUND",
                    message = $"Node not found at path: {scenePath}",
                    path = scenePath
                });
                return;
            }

            // Read request body
            using var reader = new StreamReader(request.InputStream, request.ContentEncoding);
            var body = await reader.ReadToEndAsync();
            var updateData = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(body);

            if (updateData != null && updateData.TryGetValue("properties", out var propsElement))
            {
                var properties = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(propsElement.GetRawText());
                if (properties != null)
                {
                    foreach (var (propName, propValue) in properties)
                    {
                        SetProperty(node, propName, propValue);
                    }
                }
            }

            response.StatusCode = 202;
            await WriteJsonResponse(response, new
            {
                accepted = true,
                operation = "update",
                path = scenePath,
            });
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] HandleUpdateNode error: {ex.Message}");
            response.StatusCode = 500;
            await WriteJsonResponse(response, new { error = "INTERNAL_ERROR", message = ex.Message });
        }
    }

    private async Task HandleDeleteNode(string scenePath, HttpListenerResponse response)
    {
        try
        {
            if (!scenePath.StartsWith("/"))
                scenePath = "/" + scenePath;

            var node = _sceneRead?.GetNodeByPath(scenePath);
            if (node == null)
            {
                response.StatusCode = 404;
                await WriteJsonResponse(response, new 
                { 
                    error = "NOT_FOUND",
                    message = $"Node not found at path: {scenePath}",
                    path = scenePath
                });
                return;
            }

            _pendingWrites.Enqueue(new RemoveSceneNodeCommitRequest(node.Id));

            response.StatusCode = 202;
            await WriteJsonResponse(response, new
            {
                accepted = true,
                operation = "remove",
                path = scenePath,
                nodeId = node.Id,
            });
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] HandleDeleteNode error: {ex.Message}");
            response.StatusCode = 500;
            await WriteJsonResponse(response, new { error = "INTERNAL_ERROR", message = ex.Message });
        }
    }

    private void SetProperty(INode node, string propertyName, JsonElement value)
    {
        var prop = node.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
        if (prop == null || !prop.CanWrite)
        {
            _logger?.LogWarning($"[RestAPI] Property {propertyName} not found or not writable");
            return;
        }

        try
        {
            var convertedValue = ConvertJsonElement(value, prop.PropertyType);
            _pendingWrites.Enqueue(new SetSceneNodePropertyCommitRequest(node.UniquePath, propertyName, convertedValue));
        }
        catch (Exception ex)
        {
            _logger?.LogError($"[RestAPI] SetProperty error: {ex.Message}");
        }
    }

    private object? ConvertJsonElement(JsonElement element, Type targetType)
    {
        if (targetType == typeof(Vector3))
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return new Vector3(
                    element.GetProperty("x").GetSingle(),
                    element.GetProperty("y").GetSingle(),
                    element.GetProperty("z").GetSingle()
                );
            }
        }

        return JsonSerializer.Deserialize(element.GetRawText(), targetType);
    }

    private async Task WriteJsonResponse(HttpListenerResponse response, object data)
    {
        response.ContentType = "application/json; charset=utf-8";
        var json = JsonSerializer.Serialize(data, _jsonOptions);
        var buffer = Encoding.UTF8.GetBytes(json);
        response.ContentLength64 = buffer.Length;
        await response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
        response.Close();
    }
}

/// <summary>
/// JSON converter for System.Numerics.Vector3
/// </summary>
public class Vector3JsonConverter : JsonConverter<Vector3>
{
    public override Vector3 Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException();

        float x = 0, y = 0, z = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return new Vector3(x, y, z);

            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var propertyName = reader.GetString();
                reader.Read();

                switch (propertyName?.ToLowerInvariant())
                {
                    case "x":
                        x = reader.GetSingle();
                        break;
                    case "y":
                        y = reader.GetSingle();
                        break;
                    case "z":
                        z = reader.GetSingle();
                        break;
                }
            }
        }

        throw new JsonException();
    }

    public override void Write(Utf8JsonWriter writer, Vector3 value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber("x", value.X);
        writer.WriteNumber("y", value.Y);
        writer.WriteNumber("z", value.Z);
        writer.WriteEndObject();
    }
}
