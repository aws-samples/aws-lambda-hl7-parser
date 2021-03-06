## AWS Lambda HL7 Parser

When configured as an AWS SQS destination, this Python AWS Lambda function pops incoming HL7 messages from an AWS SQS FIFO queue and parses them into a JSON object. It parses Segments, Elements and Fields. Component, sub components and repetition functionality is not currently supported.

It leverages the HL7apy library https://crs4.github.io/hl7apy/

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

