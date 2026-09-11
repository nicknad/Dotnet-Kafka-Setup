FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app
USER $APP_UID

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props ./
COPY src/KafkaPipeline.Core/KafkaPipeline.Core.csproj ./src/KafkaPipeline.Core/
COPY src/KafkaProducerApp/KafkaProducerApp.csproj ./src/KafkaProducerApp/
RUN dotnet restore "./src/KafkaProducerApp/KafkaProducerApp.csproj"
COPY src/KafkaPipeline.Core/ ./src/KafkaPipeline.Core/
COPY src/KafkaProducerApp/ ./src/KafkaProducerApp/
RUN dotnet publish "./src/KafkaProducerApp/KafkaProducerApp.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "KafkaProducerApp.dll"]
