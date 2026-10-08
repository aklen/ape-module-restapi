namespace Ape.Module.RestApi.Plugin.RestApi;

/// <summary>
/// Resolves hierarchical <c>/api/v1/scene/entities/…</c> paths:
/// trailing <c>/</c> = collection by id prefix; otherwise exact entity id.
/// </summary>
internal static class SceneEntityPathResolver
{
    /// <summary>True when the URL segment lists entities under a prefix (ends with <c>/</c>).</summary>
    public static bool IsPrefixCollection(string relativePath) =>
        relativePath.EndsWith('/');

    /// <summary>Prefix for collection URLs, e.g. <c>fleet/vehicles</c> from <c>fleet/vehicles/</c>.</summary>
    public static string NormalizePrefix(string relativePath) =>
        relativePath.TrimEnd('/');

    /// <summary>
    /// Whether <paramref name="entityId"/> belongs under <paramref name="prefix"/>.
    /// Empty prefix matches all entities.
    /// </summary>
    public static bool MatchesPrefix(string entityId, string prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return true;

        if (string.Equals(entityId, prefix, StringComparison.Ordinal))
            return true;

        return entityId.StartsWith(prefix + "/", StringComparison.Ordinal);
    }
}
