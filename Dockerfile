# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ScalableMssqlApi.csproj ./
RUN dotnet restore ScalableMssqlApi.csproj
COPY . .
RUN dotnet publish ScalableMssqlApi.csproj -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "ScalableMssqlApi.dll"]
