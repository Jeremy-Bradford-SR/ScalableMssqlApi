# Contributing

Thanks for considering contributing. This file explains how to set up a developer environment and how to propose changes.

Developer setup

1. Clone the repository and open it in your IDE of choice (Visual Studio, VS Code).

```bash
git clone <repo-url>
cd ScalableMssqlApi
```

2. Restore and run with .NET SDK:

```bash
dotnet restore
dotnet run --project ScalableMssqlApi.csproj
```

3. Or use Docker:

```bash
docker compose up --build
```

Testing and linting

This project is small and does not include automated tests. If you add tests, prefer xUnit and add a GitHub Action or similar CI.

How to contribute

- Fork the repo and open a pull request against `main`.
- Write clear commit messages and include tests for new behavior.
- Keep changes small and focused.

Code style

Follow idiomatic C# patterns and .NET Core conventions. Keep public APIs documented with XML comments where appropriate.
