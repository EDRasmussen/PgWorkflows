sample_project := "sdk-dotnet/PgWorkflows.ConsoleSample/PgWorkflows.ConsoleSample.csproj"
connection_string := "Host=localhost;Port=55432;Database=pgworkflows;Username=postgres;Password=postgres"

default:
    just --list

postgres-up:
    docker compose up -d postgres

postgres-down:
    docker compose down

postgres-logs:
    docker compose logs -f postgres

build:
    dotnet build {{sample_project}}

run: build
    PGWORKFLOWS_CONNECTION_STRING='{{connection_string}}' dotnet run --project {{sample_project}}

reset-db:
    docker compose down -v
