# REST API

**Ape.Module.RestApi** is an optional Ape module: it serves the live scene over HTTP and pushes property changes on a WebSocket. Reads come from the scene. Writes are commit requests drained on the host frame.

It is not part of Core. Core stays the scene, replica, and plugin host. This module is a vertical you add when a process needs a remote scene API.

## Place in the ecosystem

| Piece | Role |
| --- | --- |
| [ape-skeleton](https://github.com/aklen/ape-skeleton) | Workspace vessel (`./ape sync`, `./ape build`) |
| [ape-core](https://github.com/aklen/ape-core) | Framework: scene, DI, plugins, commits |
| [ape-launcher](https://github.com/aklen/ape-launcher) | Host executable |
| **this repo** | HTTP scene API and WebSocket stream |

After `./ape sync` the tree looks like:

```
ape-skeleton/
└── src/
    ├── Ape.Core/
    ├── Ape.Launcher/
    └── Ape.Modules/
        └── Ape.Module.RestApi/   ← this repository
```

Enable it in `workspace.yaml` under `modules:` (`name: Ape.Module.RestApi`, this GitHub URL). Host JSON turns the plugin on. Run from the skeleton:

```bash
./ape run -c src/Ape.Modules/Ape.Module.RestApi/Samples/rest-api.json
```

```json
"Ape.Module.RestApi": {
  "plugins": {
    "RestApi": {
      "host": "0.0.0.0",
      "port": 9090,
      "corsOrigins": ["*"]
    }
  }
}
```

The plugin key `RestApi` loads `RestApiPlugin`. Options live on that object.

| Property | Default | Meaning |
| --- | --- | --- |
| `host` | `0.0.0.0` | `0.0.0.0`, `*`, `+`, or `::` listens on every interface. A specific address binds only that one. |
| `port` | `9090` | HTTP and WebSocket share this port. |
| `corsOrigins` | `["*"]` | Browser `Origin` allow-list. `*` allows every origin. |

If the plugin section is missing, the plugin also reads `plugins/rest-api.json` from the process working directory.

`host` `0.0.0.0` tries `http://+:port/` first. When that bind fails it falls back to prefixes for localhost and each up interface. Scene-changing routes have no authentication. Restrict the port with a firewall or a trusted network. CORS does not protect non-browser clients.

## HTTP

Base path: `http://localhost:9090/api/v1/scene`

| Method | Path | Result |
| --- | --- | --- |
| `GET` | `/api/v1/scene` | Every node |
| `GET` | `/api/v1/scene/{path}` | One node |
| `PUT`, `PATCH` | `/api/v1/scene/{path}` | Enqueue property sets from `properties`. `202` when accepted |
| `DELETE` | `/api/v1/scene/{path}` | Enqueue a node remove. `202` when accepted |
| `POST` | `/api/v1/scene/{path}` | `501` — create is not implemented |
| `OPTIONS` | any | CORS preflight, `204` |

Entity ids are slash-separated paths assigned by the owning module. A trailing slash lists by id prefix. No trailing slash is an exact lookup.

| URL | Result |
| --- | --- |
| `GET /api/v1/scene/entities/` | All entities |
| `GET /api/v1/scene/entities/fleet/` | Ids under `fleet/` |
| `GET /api/v1/scene/entities/fleet/vehicles/V01` | One entity |

Each entity is an envelope plus its replicated fields. The plugin does not reference concrete entity types. It reads public properties marked with MessagePack `[Key]` — the same surface commits write — and puts them under `properties`. `id` and `entityType` stay on the envelope.

```json
{
  "id": "discovery/devices/45cfa73247702d02",
  "uniquePath": "",
  "entityType": "ape.discovery.device",
  "properties": {
    "location": "http://192.168.1.20:8080/device.xml",
    "source": "ssdp",
    "usn": "uuid:device",
    "present": true,
    "lastSeenUtc": "2026-10-08T11:47:41.746528Z"
  }
}
```

## WebSocket

`ws://localhost:9090/api/v1/scene/stream`

Client text frames use `op`: `subscribe`, `unsubscribe`, or `snapshot`. `paths` may be an entity id, a unique path, or a prefix ending in `*`. An empty subscription receives every change. The server sends `hello`, then `subscribed` / `unsubscribed` / `snapshot`, and `property_changed` when a matching replica changes.

## License

Copyright (c) 2026 [Akos Hamori](https://github.com/aklen).

Licensed under the [Mozilla Public License 2.0 (MPL-2.0)](https://www.mozilla.org/MPL/2.0/).
