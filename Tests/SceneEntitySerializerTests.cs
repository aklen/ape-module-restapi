using System.Numerics;
using System.Text.Json;
using Ape.Core.Scene.Models;
using Ape.Module.RestApi.Plugin.RestApi;
using MessagePack;

namespace Ape.Module.RestApi.Tests;

public class SceneEntitySerializerTests
{
    [Fact]
    public void Keyed_properties_land_in_the_envelope()
    {
        var seen = new DateTime(2026, 10, 8, 11, 47, 41, DateTimeKind.Utc);
        var entity = new SampleEntity
        {
            Id = "discovery/devices/abc",
            Location = "http://192.168.1.20:8080/device.xml",
            Present = true,
            Kind = SampleKind.SerialPort,
            LastSeenUtc = seen,
            Offset = new Vector3(1f, 2f, 3f),
            NotReplicated = "hidden",
        };

        using var doc = JsonDocument.Parse(SceneEntitySerializer.Serialize(SceneEntitySerializer.ToDto(entity)));
        var root = doc.RootElement;

        Assert.Equal("discovery/devices/abc", root.GetProperty("id").GetString());
        Assert.Equal("", root.GetProperty("uniquePath").GetString());
        Assert.Equal("test.sample", root.GetProperty("entityType").GetString());
        Assert.False(root.TryGetProperty("model", out _));
        Assert.False(root.TryGetProperty("extensionTypeId", out _));

        var properties = root.GetProperty("properties");
        Assert.Equal(
            ["location", "present", "kind", "lastSeenUtc", "offset", "note"],
            properties.EnumerateObject().Select(property => property.Name).ToArray());
        Assert.Equal("http://192.168.1.20:8080/device.xml", properties.GetProperty("location").GetString());
        Assert.True(properties.GetProperty("present").GetBoolean());
        Assert.Equal("serialPort", properties.GetProperty("kind").GetString());
        Assert.Equal(seen, properties.GetProperty("lastSeenUtc").GetDateTime());
        Assert.Equal(1f, properties.GetProperty("offset").GetProperty("x").GetSingle());
        Assert.Equal(2f, properties.GetProperty("offset").GetProperty("y").GetSingle());
        Assert.Equal(3f, properties.GetProperty("offset").GetProperty("z").GetSingle());
        Assert.Equal(JsonValueKind.Null, properties.GetProperty("note").ValueKind);
        Assert.False(properties.TryGetProperty("id", out _));
        Assert.False(properties.TryGetProperty("typeId", out _));
        Assert.False(properties.TryGetProperty("notReplicated", out _));
    }

    private enum SampleKind
    {
        Unknown,
        SerialPort,
    }

    private sealed class SampleEntity : Entity
    {
        public SampleEntity() : base("test.sample")
        {
        }

        [Key(10)]
        public string Location { get; set; } = "";

        [Key(11)]
        public bool Present { get; set; }

        [Key(12)]
        public SampleKind Kind { get; set; }

        [Key(13)]
        public DateTime LastSeenUtc { get; set; }

        [Key(14)]
        public Vector3 Offset { get; set; }

        [Key(15)]
        public string? Note { get; set; }

        public string NotReplicated { get; set; } = "";
    }
}
