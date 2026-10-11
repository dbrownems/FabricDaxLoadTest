using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Linq;

namespace FabricDaxLoadTest;

// `LoadGen probe` — connect as one entry of a users file and report what
// the model sees: USERPRINCIPALNAME(), CUSTOMDATA(), the roles in effect,
// and the roles defined in the model. The notebook runs it for the first
// user before every load test. See docs/loadgen-cli.md.
partial class Program
{
    static Command BuildProbeCommand()
    {
        var xmlaOption = new Option<string>("--xmla", "XMLA endpoint (e.g. powerbi://api.powerbi.com/v1.0/myorg/Workspace)") { IsRequired = true };
        var datasetOption = new Option<string>("--dataset", "Semantic model name") { IsRequired = true };
        var usersFileOption = new Option<FileInfo?>("--users-file", "Path to users.json (omit to connect without impersonation)");
        var userIndexOption = new Option<int>("--user-index", () => 0, "Zero-based entry of --users-file to connect as");
        var replicaOption = new Option<string>("--replica", () => "", "Target replica ('readonly' for scale-out read replica, or '')");
        var tokenOption = new Option<string?>("--token", "Access token (prefer --token-file or $PBI_TOKEN)");
        var tokenFileOption = new Option<FileInfo?>("--token-file", "Path to file containing access token");
        var noAuthOption = new Option<bool>("--no-auth", () => false,
            "Connect with integrated/Windows auth instead of a bearer token (local SSAS / Power BI Desktop).");
        var jsonOption = new Option<bool>("--json", () => false,
            "Emit one JSON envelope (type=probe) on stdout instead of text.");

        var command = new Command("probe",
            "Connect as one user from a users file and print USERPRINCIPALNAME(), CUSTOMDATA(), " +
            "the roles in effect for that connection, and the roles defined in the model.")
        {
            xmlaOption, datasetOption, usersFileOption, userIndexOption, replicaOption,
            tokenOption, tokenFileOption, noAuthOption, jsonOption,
        };

        command.SetHandler((InvocationContext ctx) =>
        {
            var p = ctx.ParseResult;
            ctx.ExitCode = RunProbe(
                p.GetValueForOption(xmlaOption)!,
                p.GetValueForOption(datasetOption)!,
                p.GetValueForOption(usersFileOption),
                p.GetValueForOption(userIndexOption),
                p.GetValueForOption(replicaOption) ?? "",
                p.GetValueForOption(tokenOption),
                p.GetValueForOption(tokenFileOption),
                p.GetValueForOption(noAuthOption),
                p.GetValueForOption(jsonOption));
        });
        return command;
    }

    // Exit code 0 when the user connected and the identity query ran;
    // 1 otherwise. Active-role / model-role lookups are best-effort and
    // never change the exit code.
    static int RunProbe(string xmla, string dataset, FileInfo? usersFile, int userIndex,
        string replica, string? tokenDirect, FileInfo? tokenFile, bool noAuth, bool json)
    {
        try
        {
            TextWriter info = json ? Console.Error : Console.Out;
            string token = "";
            if (!noAuth)
            {
                var resolved = ResolveToken(tokenDirect, tokenFile, info);
                if (resolved == null)
                {
                    EmitError(json, "no_token",
                        "No access token provided. Use --token, --token-file, set $PBI_TOKEN, or pass --no-auth for integrated auth.");
                    return 1;
                }
                token = resolved;
            }
            _currentToken = string.IsNullOrEmpty(token) ? null : token;

            var user = new VirtualUser("", "", "");
            if (usersFile != null)
            {
                if (!usersFile.Exists)
                {
                    EmitError(json, "users_file_not_found", $"Users file not found: {usersFile.FullName}");
                    return 1;
                }
                VirtualUser[] all;
                try { all = ParseUsers(File.ReadAllText(usersFile.FullName)); }
                catch (Exception ex)
                {
                    EmitError(json, "users_parse", $"Error parsing users file: {ex.Message}");
                    return 1;
                }
                if (userIndex < 0 || userIndex >= all.Length)
                {
                    EmitError(json, "user_index_out_of_range",
                        $"--user-index {userIndex} is out of range: the users file has {all.Length} entr{(all.Length == 1 ? "y" : "ies")}.");
                    return 1;
                }
                user = all[userIndex];
            }

            var endpoint = string.IsNullOrEmpty(replica) ? xmla : $"{xmla}?{replica}";
            var result = IdentityProbe.Run(endpoint, dataset, token,
                user.EffectiveUserName, user.CustomData, user.Roles);

            if (json)
                EmitProbeEnvelope(result, endpoint, dataset, userIndex, user);
            else
                PrintProbe(result, endpoint, dataset, userIndex, user);
            return result.Ok ? 0 : 1;
        }
        catch (Exception ex)
        {
            if (json)
                EmitErrorEnvelope("fatal", ex);
            else
                Console.Error.WriteLine($"FATAL: {Redact(ex.ToString())}");
            return 1;
        }
    }

    static void EmitProbeEnvelope(IdentityProbeResult r, string endpoint, string dataset,
        int userIndex, VirtualUser user)
    {
        EmitEnvelope(new Dictionary<string, object?>
        {
            ["type"] = "probe",
            ["ok"] = r.Ok,
            ["endpoint"] = endpoint,
            ["dataset"] = dataset,
            ["userIndex"] = userIndex,
            ["configured"] = new Dictionary<string, object?>
            {
                ["effectiveUserName"] = user.EffectiveUserName,
                ["customData"] = user.CustomData,
                ["roles"] = user.Roles,
            },
            ["connectMs"] = r.ConnectMs,
            ["userPrincipalName"] = r.UserPrincipalName,
            ["customData"] = r.CustomData,
            ["activeRoles"] = r.ActiveRoles,
            ["activeRolesError"] = r.ActiveRolesError,
            ["modelRoles"] = r.ModelRoles,
            ["modelRolesError"] = r.ModelRolesError,
            ["error"] = r.Error,
            ["errorType"] = r.ErrorType,
        });
    }

    static void PrintProbe(IdentityProbeResult r, string endpoint, string dataset,
        int userIndex, VirtualUser user)
    {
        static string OrNotSet(string s) => string.IsNullOrEmpty(s) ? "(not set)" : s;
        static string OrBlank(string? s) => string.IsNullOrEmpty(s) ? "(blank)" : s;
        // Engine errors are multi-line; keep continuation lines aligned under the value column.
        static string Block(string? s) => string.Join("\n" + new string(' ', 22),
            (s ?? "").Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0));
        // Secondary lookups only need the headline; the connect error above has the detail.
        static string FirstLine(string? s) =>
            (s ?? "").Split('\n').Select(l => l.Trim()).FirstOrDefault(l => l.Length > 0) ?? "";

        Console.WriteLine($"Model               : {dataset} @ {endpoint}");
        Console.WriteLine($"Users file [{userIndex}]      : EffectiveUserName={OrNotSet(user.EffectiveUserName)}  " +
                          $"CustomData={OrNotSet(user.CustomData)}  Roles={OrNotSet(user.Roles)}");
        if (r.Ok)
        {
            Console.WriteLine($"Connected in        : {r.ConnectMs:F0} ms");
            Console.WriteLine($"USERPRINCIPALNAME() : {OrBlank(r.UserPrincipalName)}");
            Console.WriteLine($"CUSTOMDATA()        : {OrBlank(r.CustomData)}");
            Console.WriteLine("Active roles        : " + (r.ActiveRoles == null
                ? $"(unavailable: {FirstLine(r.ActiveRolesError)})"
                : r.ActiveRoles.Length == 0
                    ? "(none)"
                    : string.Join(", ", r.ActiveRoles.Select(x =>
                        x == "*" ? "* (administrator: RLS not applied)" : x))));
        }
        else
        {
            Console.WriteLine($"Connection FAILED   : {Block(r.Error)}");
        }
        Console.WriteLine("Model roles         : " + (r.ModelRoles == null
            ? $"(unavailable: {FirstLine(r.ModelRolesError)})"
            : r.ModelRoles.Length == 0
                ? "(none defined)"
                : $"{string.Join(", ", r.ModelRoles)}  ({r.ModelRoles.Length})"));
    }
}
