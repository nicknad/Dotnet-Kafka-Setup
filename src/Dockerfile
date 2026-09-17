FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app
USER $APP_UID

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG APP
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props ./
COPY src/KafkaPipeline.Core/KafkaPipeline.Core.csproj ./src/KafkaPipeline.Core/
COPY src/${APP}/${APP}.csproj ./src/${APP}/
RUN dotnet restore "./src/${APP}/${APP}.csproj"
COPY src/KafkaPipeline.Core/ ./src/KafkaPipeline.Core/
COPY src/${APP}/ ./src/${APP}/
RUN dotnet publish "./src/${APP}/${APP}.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
ARG APP
ENV APP=${APP}
COPY --from=build /app/publish .
ENTRYPOINT ["/bin/sh", "-c", "exec dotnet ${APP}.dll"]
