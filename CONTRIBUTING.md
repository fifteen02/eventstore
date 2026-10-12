# Contributing

Thanks for helping. Open an issue first for larger changes, so we can agree on the approach.

## Build

You need Java 25, Maven and Docker (the integration tests start PostgreSQL and LavinMQ):

```bash
mvn verify
```

See [development](docs/development.md) for more.

## Pull requests

- Match the style of the surrounding code.
- Add or update tests. Pull requests without tests for new behaviour are not merged.
- Update the docs when you change behaviour or configuration.
- `mvn verify` must pass. CI runs it on every pull request.

You do not need to sign off commits (no DCO). By contributing you agree that your work is
licensed under the [Apache License 2.0](LICENSE).

Report security issues privately, as described in [SECURITY.md](SECURITY.md).
