# tap-agilecrm
A singer.io tap for AgileCRM

## installation

The `tap-agilecrm` is not yet available through `pip`, so to install one has to clone the repo and execute the following command in the root of the repo:

```bash
pip install -e .
```

## how to run

The `tap_agilecrm` requires the following three configurations to operate:

```json
{
    "api_key": "API_TOKEN_FOR_DOMAIN",
    "email": "EMAIL_ASSOCIATED_WITH_THE_TOKEN",
    "domain": "DOMAIN_ASSOCIATED_WITH_COMPANY"
}

```

## test

Test using the [`singer-tools`](https://github.com/singer-io/singer-tools) (here's how to install it: [link](https://github.com/singer-io/singer-tools#installation)).

Once installed, and the tool `singer-check-tap` is in the `$PATH`, test the tap using the following command:

```bash
tap-agilecrm -c <path_to_config_file> | singer-check-tap
```