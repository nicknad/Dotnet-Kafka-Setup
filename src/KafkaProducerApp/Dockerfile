FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Directory.Build.props .
COPY src/Directory.Packages.props .
COPY src/prime.proto .
COPY src/KafkaProducerApp/KafkaProducerApp.csproj ./KafkaProducerApp/
RUN dotnet restore "./KafkaProducerApp/KafkaProducerApp.csproj"
COPY src/KafkaProducerApp/ ./KafkaProducerApp/
WORKDIR "/src/KafkaProducerApp"
RUN dotnet build "./KafkaProducerApp.csproj" -c Release  -o /app/build

FROM base AS final
WORKDIR /app
COPY --from=build /app/build .
ENTRYPOINT ["dotnet", "KafkaProducerApp.dll"]
