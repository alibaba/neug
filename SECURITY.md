# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |

Security updates are applied only to the latest release.

## Reporting a Vulnerability

If you have discovered a security vulnerability in this project, please report it privately. **Do not disclose it as a public issue.** This gives us time to work with you to fix the issue before public exposure, reducing the chance that the exploit will be used before a patch is released.

Please report security vulnerabilities via one of the following methods:

1. **GitHub Security Advisory**: [Create a private security advisory](https://github.com/alibaba/neug/security/advisories/new)
2. **Email**: Contact the GraphScope team at graphscope@alibaba-inc.com

Please include the following information in your report:

- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Suggested fix (if any)

## Response Timeline

- **Initial Response**: We aim to acknowledge receipt of your report within 24 hours.
- **Investigation**: We will investigate and validate the issue within 2 days.
- **Resolution**: We target to provide a fix within 90 days, depending on complexity.

## Disclosure Policy

We follow a coordinated disclosure process:

1. Reporter submits vulnerability privately
2. We confirm and investigate the issue
3. We develop and test a fix
4. We release the fix and publish a security advisory
5. Reporter may then disclose publicly (after fix is released)

## Security Best Practices

When using NeuG in production:

- Keep NeuG updated to the latest version
- Use appropriate access controls for database files
- Validate and sanitize all user inputs before query execution
- Follow the principle of least privilege for service deployments

## Acknowledgments

We appreciate the security research community's efforts in responsibly disclosing vulnerabilities. Contributors who report valid security issues will be acknowledged in release notes (unless they prefer to remain anonymous).
