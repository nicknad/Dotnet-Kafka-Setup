FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app
USER $APP_UID

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props ./
COPY src/KafkaPipeline.Core/KafkaPipeline.Core.csproj ./src/KafkaPipeline.Core/
COPY src/KafkaConsumerApp/KafkaConsumerApp.csproj ./src/KafkaConsumerApp/
RUN dotnet restore "./src/KafkaConsumerApp/KafkaConsumerApp.csproj"
COPY src/KafkaPipeline.Core/ ./src/KafkaPipeline.Core/
COPY src/KafkaConsumerApp/ ./src/KafkaConsumerApp/
RUN dotnet publish "./src/KafkaConsumerApp/KafkaConsumerApp.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "KafkaConsumerApp.dll"]
