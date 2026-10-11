using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.AnalysisServices.AdomdClient;

namespace FabricDaxLoadTest
{
    /// <summary>
    /// What the semantic model sees for one virtual user's impersonation
    /// settings. Produced by <see cref="IdentityProbe.Run"/>.
    /// </summary>
    public sealed class IdentityProbeResult
    {
        /// <summary>Time to open the impersonated connection, in ms.</summary>
        public double ConnectMs { get; set; }

        /// <summary><c>USERPRINCIPALNAME()</c> on the impersonated connection.</summary>
        public string? UserPrincipalName { get; set; }

        /// <summary><c>CUSTOMDATA()</c> on the impersonated connection.</summary>
        public string? CustomData { get; set; }

        /// <summary>
        /// Roles in effect for the impersonated session: the <c>ROLES</c>
        /// column of <c>$SYSTEM.DBSCHEMA_CATALOGS</c> for the target model,
        /// minus session-property entries such as <c>CustomData=…</c>.
        /// <c>["*"]</c> means the session has administrator rights on the
        /// model, so RLS is not applied. Null when unavailable.
        /// </summary>
        public string[]? ActiveRoles { get; set; }
        public string? ActiveRolesError { get; set; }

        /// <summary>
        /// Every role defined in the model (<c>$SYSTEM.TMSCHEMA_ROLES</c>).
        /// Read without impersonation because it needs write/admin
        /// permission on the model. Null when unavailable.
        /// </summary>
        public string[]? ModelRoles { get; set; }
        public string? ModelRolesError { get; set; }

        /// <summary>Connect / identity-query failure on the impersonated connection; null on success.</summary>
        public string? Error { get; set; }
        public string? ErrorType { get; set; }

        public bool Ok => Error == null;
    }

    /// <summary>
    /// Connects to the model as one virtual user — with the same connection
    /// string a load-test user gets — and reports the identity, CustomData,
    /// and roles the engine applies. Used to sanity-check a users file
    /// before a run.
    /// </summary>
    public static class IdentityProbe
    {
        public const string IdentityQuery =
            "EVALUATE ROW(\"UserPrincipalName\", USERPRINCIPALNAME(), \"CustomData\", CUSTOMDATA())";
        public const string ActiveRolesQuery =
            "SELECT [CATALOG_NAME], [ROLES] FROM $SYSTEM.DBSCHEMA_CATALOGS";
        public const string ModelRolesQuery =
            "SELECT [Name] FROM $SYSTEM.TMSCHEMA_ROLES";

        // Probe queries are tiny; don't inherit the load test's 2h command timeout.
        private const int CommandTimeoutSec = 120;

        public static IdentityProbeResult Run(
            string xmlaEndpoint, string dataset, string token,
            string? effectiveUserName, string? customData, string? roles)
        {
            effectiveUserName ??= "";
            customData ??= "";
            roles ??= "";
            bool impersonated = effectiveUserName.Length > 0 || customData.Length > 0 || roles.Length > 0;
            var result = new IdentityProbeResult();

            AdomdConnection? conn = null;
            try
            {
                try
                {
                    var sw = Stopwatch.StartNew();
                    conn = new AdomdConnection(QueryRunner.BuildConnectionString(
                        xmlaEndpoint, dataset, token, effectiveUserName, customData, roles, "probe"));
                    conn.Open();
                    result.ConnectMs = Math.Round(sw.Elapsed.TotalMilliseconds, 1);

                    using var cmd = NewCommand(IdentityQuery, conn);
                    using var rdr = cmd.ExecuteReader();
                    if (rdr.Read())
                    {
                        result.UserPrincipalName = AsString(rdr.GetValue(0));
                        result.CustomData = AsString(rdr.GetValue(1));
                    }
                }
                catch (Exception ex)
                {
                    result.Error = QueryRunner.RedactToken(ex.Message, token);
                    result.ErrorType = ex.GetType().FullName;
                }

                if (result.Ok)
                {
                    try { result.ActiveRoles = ReadActiveRoles(conn!, dataset); }
                    catch (Exception ex) { result.ActiveRolesError = QueryRunner.RedactToken(ex.Message, token); }
                }

                try
                {
                    if (impersonated)
                    {
                        // Impersonation drops admin rights, so list the
                        // model's roles as the token's own principal.
                        using var plain = new AdomdConnection(QueryRunner.BuildConnectionString(
                            xmlaEndpoint, dataset, token, "", "", "", "probe"));
                        plain.Open();
                        result.ModelRoles = ReadModelRoles(plain);
                    }
                    else if (result.Ok)
                    {
                        result.ModelRoles = ReadModelRoles(conn!);
                    }
                    else
                    {
                        result.ModelRolesError = "could not connect";
                    }
                }
                catch (Exception ex)
                {
                    // A server-side error here almost always means the
                    // token's principal lacks write/admin on the model.
                    result.ModelRolesError =
                        (ex is AdomdErrorResponseException
                            ? "listing roles needs write/admin permission on the model: "
                            : "")
                        + QueryRunner.RedactToken(ex.Message, token);
                }
            }
            finally
            {
                try { conn?.Dispose(); } catch { }
            }
            return result;
        }

        private static string[] ReadActiveRoles(AdomdConnection conn, string dataset)
        {
            using var cmd = NewCommand(ActiveRolesQuery, conn);
            using var rdr = cmd.ExecuteReader();
            string? match = null, last = null;
            int rows = 0;
            while (rdr.Read())
            {
                rows++;
                last = AsString(rdr.GetValue(1)) ?? "";
                if (string.Equals(AsString(rdr.GetValue(0)), dataset, StringComparison.OrdinalIgnoreCase))
                {
                    match = last;
                    break;
                }
            }
            var raw = match ?? (rows == 1 ? last : null)
                ?? throw new InvalidOperationException(
                    $"$SYSTEM.DBSCHEMA_CATALOGS has no row for '{dataset}' ({rows} row(s) returned).");
            var entries = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            // For an administrator the server lists "*" followed by every
            // role in the model, but none of them filters anything.
            if (Array.IndexOf(entries, "*") >= 0)
                return new[] { "*" };
            // The server also appends session properties such as
            // "CustomData=USA"; those aren't roles.
            return Array.FindAll(entries, e => !IsSessionPropertyEntry(e));
        }

        private static bool IsSessionPropertyEntry(string entry) =>
            entry.StartsWith("CustomData=", StringComparison.OrdinalIgnoreCase)
            || entry.StartsWith("EffectiveUserName=", StringComparison.OrdinalIgnoreCase)
            || entry.StartsWith("UserName=", StringComparison.OrdinalIgnoreCase);

        private static string[] ReadModelRoles(AdomdConnection conn)
        {
            using var cmd = NewCommand(ModelRolesQuery, conn);
            using var rdr = cmd.ExecuteReader();
            var names = new List<string>();
            while (rdr.Read())
            {
                var name = AsString(rdr.GetValue(0));
                if (!string.IsNullOrEmpty(name)) names.Add(name);
            }
            names.Sort(StringComparer.OrdinalIgnoreCase);
            return names.ToArray();
        }

        private static AdomdCommand NewCommand(string text, AdomdConnection conn) =>
            new AdomdCommand(text, conn) { CommandTimeout = CommandTimeoutSec };

        private static string? AsString(object? value) =>
            value is null || value is DBNull ? null : value.ToString();
    }
}
