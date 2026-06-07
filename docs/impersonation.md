# User impersonation (RLS testing)

`FabricDaxLoadTest` can drive each virtual user with a different identity
so a single load test can exercise role-based security (RLS) the same way
a mixed real-user workload would. Three connection-string properties are
supported, in any combination:

| Property            | Effect                                                                 |
| ------------------- | ---------------------------------------------------------------------- |
| `EffectiveUserName` | Sets the connecting identity. `USERPRINCIPALNAME()` reflects this.     |
| `CustomData`        | Sets the value `CUSTOMDATA()` returns. Independent of identity / RLS.  |
| `Roles`             | Comma-separated list of role names to apply (subset of effective set). |

## `users.json` v2 schema

The notebook reads the file pointed to by `USERS_FILE` (Resources panel)
or the inline `USERS_INLINE` cell. The local CLI reads `--users-file`.
Three shapes are accepted, all in the same array:

```jsonc
[
  // 1. String shorthand → EffectiveUserName, no CustomData, no Roles
  "alice@contoso.com",

  // 2. Full v2 form. Roles can be a string or a JSON array.
  {
    "effectiveUserName": "bob@contoso.com",
    "customData":        "tenant-42",
    "roles":             ["Sales East", "Sales West"]
  },

  // 3. CustomData-only — useful for testing CUSTOMDATA()-driven RLS
  //    without granting real users access to the model.
  { "customData": "USA" },

  // 4. Legacy v0.4.x form (still accepted as deprecated aliases).
  //    "email" → CustomData, "role" → Roles. NOT EffectiveUserName.
  { "email": "USA", "role": "USA Role" }
]
```

Keys are case-insensitive. An entry with no recognized field becomes a
slot with no impersonation (full token identity). Empty `[]` falls back
to a single anonymous slot.

> **Breaking change vs. v0.4.x**: a flat string array used to map each
> entry to `CustomData=` (via the misnamed `email` field). It now maps to
> `EffectiveUserName=`. Existing files that use the explicit
> `{"email": "...", "role": "..."}` form keep their old behaviour because
> `email` is preserved as a deprecated alias for `customData`.

## Combination semantics (verified against the PBI XMLA endpoint)

| EUN | Roles | CustomData | Result                                                                  |
| --- | ----- | ---------- | ----------------------------------------------------------------------- |
| –   | –     | –          | Connecting principal (your token). Admin sees everything.               |
| ✓   | –     | –          | Identity = EUN. RLS = EUN's actual role membership.                     |
| –   | ✓     | –          | Identity = connecting principal. Workspace admins can pick any role; non-admins must be members. |
| –   | –     | ✓          | `CUSTOMDATA()` returns the value. Identity / RLS unchanged.             |
| ✓   | ✓     | –          | Identity = EUN. **EUN must be a member of every named role** — non-member roles are rejected at connect time with `user does not have access to the database`. Use this to *narrow* RLS to a subset of EUN's real roles. |
| ✓   | –     | ✓          | Identity = EUN; `CUSTOMDATA()` set independently.                       |
| –   | ✓     | ✓          | Roles applied to connecting principal; `CUSTOMDATA()` set.              |
| ✓   | ✓     | ✓          | All three apply with the rules above.                                   |

Multiple roles are an OR (union), matching standard RLS semantics. With
EUN, the connection string can only narrow to a subset of EUN's actual
roles — you cannot force-fit EUN into a role they're not really a member
of.

For testing arbitrary role behaviour without granting real users the
role, use `CustomData` and write the role's filter expression in DAX as
`CUSTOMDATA()`-driven (rather than user-driven). This is the cleanest
pattern for targeted RLS regression tests.

## Permissions on the model

- The connecting principal (your bearer token) needs **Build** permission
  on the model.
- Specifying `EffectiveUserName=foo@…` additionally requires that user to
  have **Build** permission on the model. Without it, the connection
  fails immediately.
- A workspace admin can specify any role via `Roles=` for themselves.
  A non-admin can only specify roles they're a member of, regardless of
  whether `EffectiveUserName` is set.

## Token acquisition gotcha (local CLI)

If you're running the CLI outside Fabric and minting a token yourself,
the **Azure CLI's app id** (`04b07795-…`) gets a token with the right
audience but **the PBI XMLA endpoint refuses to accept it**, even though
the same token works for the PBI REST API. Symptom:

```
AdomdConnectionException: Authentication failed for all authenticators
```

Token from the `fab` CLI's MSAL cache (app id `5814bfb4-…`) **is**
accepted. After `fab auth login`:

```pwsh
$env:PBI_TOKEN = python -c "from fabric_cli.core import fab_auth; print(fab_auth.FabAuth().get_access_token(['https://analysis.windows.net/powerbi/api/.default'], False))"
```

Inside a Fabric notebook this is a non-issue: `notebookutils.credentials.getToken('pbi')`
returns a token the XMLA endpoint accepts.
