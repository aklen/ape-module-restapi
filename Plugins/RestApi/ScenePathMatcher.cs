namespace Ape.Module.RestApi.Plugin.RestApi;

/// <summary>Matches scene entity id / uniquePath against subscribe patterns (prefix + trailing *).</summary>
public static class ScenePathMatcher
{
    public static bool MatchesAny(IEnumerable<string> patterns, string entityId, string uniquePath)
    {
        foreach (var pattern in patterns)
        {
            if (Matches(pattern, entityId, uniquePath))
                return true;
        }

        return false;
    }

    public static bool Matches(string pattern, string entityId, string uniquePath)
    {
        if (string.IsNullOrWhiteSpace(pattern) || pattern is "*" or "/*")
            return true;

        var normalized = pattern.Trim();
        if (MatchesSingle(normalized, entityId) || MatchesSingle(normalized, uniquePath))
            return true;

        var trimmed = normalized.TrimStart('/');
        return MatchesSingle(trimmed, entityId) || MatchesSingle(trimmed, uniquePath.TrimStart('/'));
    }

    private static bool MatchesSingle(string pattern, string value)
    {
        if (pattern.EndsWith('*'))
        {
            var prefix = pattern[..^1];
            return value.StartsWith(prefix, StringComparison.Ordinal);
        }

        return string.Equals(pattern, value, StringComparison.Ordinal) ||
               string.Equals("/" + pattern, value, StringComparison.Ordinal);
    }
}
