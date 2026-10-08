namespace Ape.Module.RestApi.Plugin.RestApi;

internal sealed class SceneStreamClientMessage
{
    public string Op { get; init; } = "";
    public List<string> Paths { get; init; } = [];
}

internal sealed class SceneStreamServerMessage
{
    public string Type { get; init; } = "";
    public int Version { get; init; } = 1;
    public List<string>? Paths { get; init; }
    public string? Path { get; init; }
    public string? ReplicaId { get; init; }
    public string? PropertyName { get; init; }
    public SceneEntityDto? Entity { get; init; }
    public List<SceneEntityDto>? Entities { get; init; }
}
