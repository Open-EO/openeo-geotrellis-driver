## Deprecated OpenEO Geotrellis Driver

[![Status](https://img.shields.io/badge/Status-proof--of--concept-yellow.svg)]()

This driver implements the Geotrellis specific backend for OpenEO. It's currently replaced by a driver which uses GeoPySpark, which is a Python wrapper around Geotrellis.

### Implementation details
The REST API tries to use standardized Java API's, such as JAX-RS for REST services
and [json processing](https://jcp.org/en/jsr/detail?id=353) for generic JSON parsing.
