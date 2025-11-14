FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Directory.Build.props .
COPY src/Directory.Packages.props .
COPY src/prime.proto .
COPY src/KafkaConsumerApp/KafkaConsumerApp.csproj ./KafkaConsumerApp/
RUN dotnet restore "./KafkaConsumerApp/KafkaConsumerApp.csproj"
COPY src/KafkaConsumerApp/ ./KafkaConsumerApp/
WORKDIR "/src/KafkaConsumerApp"
RUN dotnet build "./KafkaConsumerApp.csproj" -c Release  -o /app/build


FROM base AS final
WORKDIR /app
COPY --from=build /app/build .
ENTRYPOINT ["dotnet", "KafkaConsumerApp.dll"]
