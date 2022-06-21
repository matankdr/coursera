resolvers in ThisBuild += "lightbend-commercial-mvn" at
        "https://repo.lightbend.com/pass/4pTBwaPEN02UjiYj9T5GlX9-kALswbeYrT-j1hMh5pqRz3eF/commercial-releases"
resolvers in ThisBuild += Resolver.url("lightbend-commercial-ivy",
        url("https://repo.lightbend.com/pass/4pTBwaPEN02UjiYj9T5GlX9-kALswbeYrT-j1hMh5pqRz3eF/commercial-releases"))(Resolver.ivyStylePatterns)
