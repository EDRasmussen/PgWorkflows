# Regenerate sdk-dotnet/PgWorkflows/PublicAPI.Unshipped.txt after intentionally
# adding/changing public API (fixes the RS0016/RS0017 build warnings). If the codefix
# leaves warnings behind, copy the symbol names from the build output into the txt file.
update-public-api:
    dotnet format analyzers sdk-dotnet/PgWorkflows/PgWorkflows.csproj --diagnostics RS0016 RS0017
