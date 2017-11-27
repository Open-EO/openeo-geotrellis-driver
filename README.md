## OpenEO Geotrellis Driver
Proof of concept

This driver implements the Geotrellis specific backend for OpenEO.

###Implementation details
The REST API tries to use standardized Java API's, such as JAX-RS for REST services
and json processing (https://jcp.org/en/jsr/detail?id=353) for generic JSON parsing.
