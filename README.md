# Stack Exchange Computer Science User Service

This is a sample decentralized user service developed using the Chord algorithm using user profile data from the cs.stackexchange.com group.

Given a user ID, a client fetches a user profile in a two step process:
1. The client issues a lookup request to any node in the Chord ring and receives the IP address of the responsible node
2. The client issues a fetch request to the responsible node, receiving User data in response.

## License

The Stack Exchange Network data used in this licensed was released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/). It was downloaded from [archive.org](https://archive.org/details/stackexchange) as XML data, and subsequently converted to JSON. The derived Users.json file is thus also released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/) with identical conditions.

The remainder of the application logic is licensed under under the terms of the MIT license.