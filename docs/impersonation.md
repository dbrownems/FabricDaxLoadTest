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
  { "customData": "USA" }
]
```

Keys are case-insensitive. An entry with no recognized field becomes a
slot with no impersonation (full token identity). Empty `[]` falls back
to a single anonymous slot.

## Combination semantics (verified against the PBI XMLA endpoint)

| EUN | Roles | CustomData | Result                                                                  |
| --- | ----- | ---------- | ----------------------------------------------------------------------- |
| –   | –     | –          | Connecting principal (your token). Admin sees everything.               |
| ✓   | –     | –          | Identity = EUN. RLS = EUN's actual role membership. **Caveat:** if EUN equals the connecting admin's own UPN, admin bypass still applies and RLS is NOT triggered (see below). |
| –   | ✓     | –          | Identity = connecting principal. Workspace admins can pick any role; non-admins must be members. |
| –   | –     | ✓          | `CUSTOMDATA()` returns the value. Identity / RLS unchanged.             |
| ✓   | ✓     | –          | Identity = EUN. **EUN must be a member of every named role** — non-member roles are rejected at connect time with `user does not have access to the database`. Use this to *narrow* RLS to a subset of EUN's real roles. **This rule applies even when EUN = the connecting admin's own UPN — admin bypass is dropped once `Roles=` is set with EUN.** |
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

## Gotcha: `EffectiveUserName=<your own UPN>` does NOT trigger RLS for admins

A semantic-model admin (workspace admin, dataset Build/Owner, or capacity
admin acting as one) **bypasses RLS unconditionally** on direct
connections. Setting `EffectiveUserName=` to your *own* UPN does **not**
re-enter RLS — the admin bypass still wins.

Verified in workspace `dbrowne-loadtest`, dataset `DIAD Final Report with
RLS` (7 country roles, none of which `davidbrowne@powerbicat.net` is a
member of):

| Test | Connection                            | sales_rows | geo_rows |
| ---- | ------------------------------------- | ---------: | -------: |
| T1   | admin token, no EUN, no Roles         |  7,235,490 |  176,931 |
| T2   | admin token, EUN=davidbrowne@…        |  7,235,490 |  176,931 |
| T3   | admin token, EUN=davidbrowne@… + Roles=`USA Role` | *connect rejected: "user does not have access to the database"* | — |
| T4   | admin token, Roles=`USA Role` (no EUN)|  4,198,753 |   39,948 |

T1 ≡ T2 → admin bypass was preserved through EUN-self. T3 → setting
`Roles=` alongside EUN-self drops admin bypass and the role-membership
check applies. T4 → admin without EUN can pick any role and gets its
filter, because admins can short-circuit the membership check on
`Roles=` alone.

**Implication for load testing.** If you want to drive *yourself* as a
non-admin RLS subject (e.g. reproduce a customer-reported slow query
under their role), you must either:

* set `Roles=<role-name>` *without* `EffectiveUserName` (relies on the
  admin role-pick privilege), or
* set both `EffectiveUserName=<your-upn>` **and** `Roles=<role>` where
  you are a real member of `<role>` (admin bypass is dropped).

The simplest and most realistic option is **CustomData-driven RLS**: a
role whose filter expression reads `CUSTOMDATA()`. You can then set
`CustomData=USA` (or whatever) without needing an EUN at all — admin
bypass applies to identity, but a `CUSTOMDATA()`-keyed filter still
evaluates because it's a plain DAX expression, not a role-membership
check. This is the cleanest pattern for RLS regression tests under load.

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
