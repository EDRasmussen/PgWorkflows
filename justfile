# Regenerate sdk-dotnet/PgWorkflows/PublicAPI.Unshipped.txt after intentionally
# adding/changing public API (fixes the RS0016 build warnings).
update-public-api:
    dotnet format analyzers sdk-dotnet/PgWorkflows/PgWorkflows.csproj --diagnostics RS0016
