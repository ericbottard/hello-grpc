FROM scratch

ADD /target/server /server

EXPOSE 8080

ENTRYPOINT ["/server", "8080"]
