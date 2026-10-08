using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ape.Core.Replication;
using Ape.Core.Scene;
using MessagePack;

namespace Ape.Module.RestApi.Plugin.RestApi;

internal sealed class SceneEntityDto
{
    public string Id { get; init; } = "";
    public string UniquePath { get; init; } = "";
    public string EntityType { get; init; } = "";
    public Dictionary<string, JsonElement> Properties { get; init; } = new();
}

internal static class SceneEntitySerializer
{
    private static readonly JsonSerializerOptions JsonOptions = CreateOptions();
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> KeyedProperties = new();

    public static JsonSerializerOptions Options => JsonOptions;

    public static SceneEntityDto ToDto(IEntity entity)
    {
        var replica = (IReplica)entity;
        var properties = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in KeyedProperties.GetOrAdd(entity.GetType(), DiscoverKeyedProperties))
        {
            var name = JsonOptions.PropertyNamingPolicy!.ConvertName(property.Name);
            var value = property.GetValue(entity);
            properties[name] = JsonSerializer.SerializeToElement(value, property.PropertyType, JsonOptions);
        }

        return new SceneEntityDto
        {
            Id = replica.Id,
            UniquePath = replica.UniquePath,
            EntityType = entity.TypeId,
            Properties = properties,
        };
    }

    public static string Serialize(object payload) =>
        JsonSerializer.Serialize(payload, JsonOptions);

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        options.Converters.Add(new Vector3JsonConverter());
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        return options;
    }

    /// <summary>
    /// Public replicated fields: the same <see cref="KeyAttribute"/> surface commits write.
    /// Identity stays on the envelope, so <c>Id</c> and <c>TypeId</c> are left out of <c>properties</c>.
    /// </summary>
    private static PropertyInfo[] DiscoverKeyedProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property =>
                property.CanRead &&
                property.GetIndexParameters().Length == 0 &&
                property.Name is not ("Id" or "TypeId") &&
                property.GetCustomAttribute<KeyAttribute>() != null)
            .OrderBy(property => property.GetCustomAttribute<KeyAttribute>()!.IntKey ?? int.MaxValue)
            .ThenBy(property => property.Name, StringComparer.Ordinal)
            .ToArray();
}
