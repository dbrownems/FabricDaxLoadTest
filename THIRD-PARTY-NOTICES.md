# Third-party notices

The `fdlt_runtime` Python wheel published from this repository bundles
.NET assemblies that are **not** licensed under the MIT license that
covers the FabricDaxLoadTest source code. They are included verbatim
from the corresponding NuGet packages so the load-test driver
(`LoadGen.dll`) can run unmodified inside Microsoft Fabric Spark
notebooks.

| Component | Source | License |
|---|---|---|
| `Microsoft.AnalysisServices.AdomdClient.dll` | [`Microsoft.AnalysisServices.AdomdClient.NetCore.retail.amd64`](https://www.nuget.org/packages/Microsoft.AnalysisServices.AdomdClient.NetCore.retail.amd64) | [Microsoft Software License](https://go.microsoft.com/fwlink/?LinkId=529443) |
| `Microsoft.AnalysisServices.Runtime.Core.dll` | same NuGet package as above | same |
| `Microsoft.AnalysisServices.Runtime.Windows.dll` | same NuGet package as above | same |
| `Microsoft.Identity.Client.dll` | [`Microsoft.Identity.Client`](https://www.nuget.org/packages/Microsoft.Identity.Client) | [MIT](https://github.com/AzureAD/microsoft-authentication-library-for-dotnet/blob/main/LICENSE) |
| `Microsoft.IdentityModel.Abstractions.dll` | [`Microsoft.IdentityModel.Abstractions`](https://www.nuget.org/packages/Microsoft.IdentityModel.Abstractions) | [MIT](https://github.com/AzureAD/azure-activedirectory-identitymodel-extensions-for-dotnet/blob/main/LICENSE.txt) |
| `System.CommandLine.dll` | [`System.CommandLine`](https://www.nuget.org/packages/System.CommandLine) | [MIT](https://github.com/dotnet/command-line-api/blob/main/LICENSE.md) |

The first three rows above (the ADOMD client + Analysis Services
runtime DLLs) are governed by the Microsoft Software License terms
linked above, which permit redistribution as part of an application
that uses the client to talk to Microsoft Analysis Services / Power
BI / Fabric. They are bundled here for that purpose.

The remaining MIT-licensed DLLs are redistributed under their original
MIT licenses; the upstream license texts are linked from the table.

The `LoadGen.dll`, `QueryRunner.dll`, `LoadGen.deps.json`, and
`LoadGen.runtimeconfig.json` files inside the wheel's `loadgen/`
folder are built from the source in this repository and are covered
by the repository's [LICENSE](LICENSE) (MIT).

If you redistribute the wheel further, retain this notice and the
upstream license links.
