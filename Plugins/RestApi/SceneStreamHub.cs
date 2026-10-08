using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Ape.Core.Logging;
using Ape.Core.Replication;
using Ape.Core.Scene;

namespace Ape.Module.RestApi.Plugin.RestApi;

/// <summary>WebSocket scene stream: path subscribe + PropertyChanged push.</summary>
internal sealed class SceneStreamHub
{
    private readonly ISceneRead _sceneRead;
    private readonly ILogger? _logger;
    private readonly ConcurrentDictionary<Guid, SceneStreamClient> _clients = new();

    public SceneStreamHub(ISceneRead sceneRead, ILogger? logger)
    {
        _sceneRead = sceneRead;
        _logger = logger;
    }

    public int ClientCount => _clients.Count;

    public void OnPropertyChanged(PropertyChangedEvent evt)
    {
        var entity = _sceneRead.GetEntity(evt.ReplicaId);
        if (entity == null)
            return;

        var dto = SceneEntitySerializer.ToDto(entity);
        foreach (var client in _clients.Values)
        {
            if (!client.Matches(dto.Id, dto.UniquePath))
                continue;

            client.TrySend(new SceneStreamServerMessage
            {
                Type = "property_changed",
                Path = dto.UniquePath,
                ReplicaId = dto.Id,
                PropertyName = evt.PropertyName,
                Entity = dto,
            });
        }
    }

    public async Task HandleClientAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var client = new SceneStreamClient(socket, _logger);
        _clients[client.Id] = client;
        _logger?.LogInfo($"[SceneStream] client connected ({_clients.Count})");

        try
        {
            client.TrySend(new SceneStreamServerMessage { Type = "hello", Version = 1 });

            var buffer = new byte[4096];
            while (socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                var result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                if (result.MessageType != WebSocketMessageType.Text)
                    continue;

                var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                HandleClientMessage(client, json);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (WebSocketException ex)
        {
            _logger?.LogDebug($"[SceneStream] socket ended: {ex.Message}");
        }
        finally
        {
            _clients.TryRemove(client.Id, out _);
            try
            {
                if (socket.State == WebSocketState.Open || socket.State == WebSocketState.CloseReceived)
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None)
                        .ConfigureAwait(false);
            }
            catch
            {
            }

            socket.Dispose();
            _logger?.LogInfo($"[SceneStream] client disconnected ({_clients.Count})");
        }
    }

    private void HandleClientMessage(SceneStreamClient client, string json)
    {
        SceneStreamClientMessage? message;
        try
        {
            message = JsonSerializer.Deserialize<SceneStreamClientMessage>(
                json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
        catch
        {
            return;
        }

        if (message == null || string.IsNullOrWhiteSpace(message.Op))
            return;

        switch (message.Op.ToLowerInvariant())
        {
            case "subscribe":
                client.Subscribe(message.Paths);
                client.TrySend(new SceneStreamServerMessage
                {
                    Type = "subscribed",
                    Paths = client.Paths.ToList(),
                });
                SendSnapshot(client);
                break;

            case "unsubscribe":
                client.Unsubscribe(message.Paths);
                client.TrySend(new SceneStreamServerMessage
                {
                    Type = "unsubscribed",
                    Paths = client.Paths.ToList(),
                });
                break;

            case "snapshot":
                SendSnapshot(client);
                break;
        }
    }

    private void SendSnapshot(SceneStreamClient client)
    {
        var entities = _sceneRead.GetAllEntities()
            .Select(SceneEntitySerializer.ToDto)
            .Where(dto => client.Matches(dto.Id, dto.UniquePath))
            .ToList();

        client.TrySend(new SceneStreamServerMessage
        {
            Type = "snapshot",
            Entities = entities,
        });
    }

    private sealed class SceneStreamClient
    {
        private readonly WebSocket _socket;
        private readonly ILogger? _logger;
        private readonly object _sendGate = new();
        private readonly HashSet<string> _paths = new(StringComparer.Ordinal);

        public SceneStreamClient(WebSocket socket, ILogger? logger)
        {
            _socket = socket;
            _logger = logger;
            Id = Guid.NewGuid();
        }

        public Guid Id { get; }

        public IReadOnlyCollection<string> Paths
        {
            get
            {
                lock (_paths)
                    return _paths.ToList();
            }
        }

        public void Subscribe(IEnumerable<string> paths)
        {
            lock (_paths)
            {
                foreach (var path in paths)
                {
                    if (!string.IsNullOrWhiteSpace(path))
                        _paths.Add(path.Trim());
                }
            }
        }

        public void Unsubscribe(IEnumerable<string> paths)
        {
            lock (_paths)
            {
                foreach (var path in paths)
                    _paths.Remove(path.Trim());
            }
        }

        public bool Matches(string entityId, string uniquePath)
        {
            lock (_paths)
            {
                if (_paths.Count == 0)
                    return true;
                return ScenePathMatcher.MatchesAny(_paths, entityId, uniquePath);
            }
        }

        public void TrySend(SceneStreamServerMessage message)
        {
            if (_socket.State != WebSocketState.Open)
                return;

            try
            {
                var json = SceneEntitySerializer.Serialize(message);
                var bytes = Encoding.UTF8.GetBytes(json);
                lock (_sendGate)
                {
                    _ = _socket.SendAsync(
                        bytes,
                        WebSocketMessageType.Text,
                        endOfMessage: true,
                        CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"[SceneStream] send failed: {ex.Message}");
            }
        }
    }
}
