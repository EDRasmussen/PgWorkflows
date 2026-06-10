# Build context must be the repo root (the example references ../sdk-dotnet).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY sdk-dotnet/PgWorkflows/ sdk-dotnet/PgWorkflows/
COPY example/Example.Workflows/ example/Example.Workflows/
COPY example/Example.Api/ example/Example.Api/
RUN dotnet publish example/Example.Api/Example.Api.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Example.Api.dll"]
