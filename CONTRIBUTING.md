# Contributing to FoundationDB

Welcome to the FoundationDB community, and thank you for contributing! The following guide outlines the basics of how to get involved.

If you have questions, we encourage you to engage in discussion on the [community forums](https://forums.foundationdb.org). Pull requests to update and expand this guide are also welcome.

#### Table of Contents

* [Before you get started](#before-you-get-started)
	* [Community Guidelines](#community-guidelines)
	* [Project Licensing](#project-licensing)
	* [Governance and Decision Making](#governance-and-decision-making)
* [Contributing](#contributing)
	* [Opening a Pull Request](#opening-a-pull-request)
	* [Reporting Issues](#reporting-issues)
* [Project Communication](#project-communication)
	* [Community Forums](#community-forums)
	* [Using GitHub Issues and Community Forums](#using-github-issues-and-community-forums)
	* [Project and Development Updates](#project-and-development-updates)

## Before you get started
### Community Guidelines
We want the FoundationDB community to be as welcoming and inclusive as possible, and have adopted a [Code of Conduct](CODE_OF_CONDUCT.md) that we ask all community members to read and observe.

### Project Licensing
By submitting a pull request, you represent that you have the right to license your contribution to Apple and the community, and agree by submitting the patch that your contributions are licensed under the Apache 2.0 license.

### Governance and Decision Making
At project launch, FoundationDB has a light governance structure. The intention is for the community to evolve quickly and adopt additional processes as participation grows. Stay tuned, and stay engaged! Your feedback is welcome.

We draw inspiration from the Apache Software Foundation's informal motto: ["community over code"](https://blogs.apache.org/foundation/entry/asf_15_community_over_code), and their emphasis on meritocratic rules. You'll also observe that some initial community structure is [inspired by the Swift community](https://swift.org/community/#community-structure).

The project technical lead is Evan Tschannen (ejt@apple.com).

Members of the Apple FoundationDB team are part of the core committers helping review individual contributions; you'll see them commenting on your pull requests.  As the FDB open source community has grown, some members of the community have consistently produced high quality code reviews and other significant contributions to FoundationDB.  The project technical lead maintains a list of external committers that actively contribute in this way, and gives them permission to review and merge pull requests.

## Contributing
### Opening a Pull Request
We love pull requests! For minor changes, feel free to open up a PR directly. For larger feature development and any changes that may require community discussion, we ask that you discuss your ideas on the [community forums](https://forums.foundationdb.org) prior to opening a PR, and then reference that thread within your PR comment.

CI will be run automatically for core committers, and for community PRs it will be initiated by the request of a core committer.  Tests can also be run locally via `ctest`, and core committers can run additional validation on pull requests prior to merging them.

### Reporting issues
Please refer to the section below on [using GitHub issues and the community forums](#using-github-issues-and-community-forums) for more info.

#### Security issues
To report a security issue, please **DO NOT** start by filing a public issue or posting to the forums; instead send a private email to [fdb-oss-security@group.apple.com](mailto:fdb-oss-security@group.apple.com).

## Project Communication
### Community Forums
We encourage your participation asking questions and helping improve the FoundationDB project. Check out the [FoundationDB community forums](https://forums.foundationdb.org), which serve a similar function as mailing lists in many open source projects. The forums are organized into three sections:

* [Development](https://forums.foundationdb.org/c/development): For discussing the internals and development of the FoundationDB core, as well as layers.
* [Using FoundationDB](https://forums.foundationdb.org/c/using-foundationdb): For discussing user-facing topics. Getting started and have a question? This is the place for you.
* [Site Feedback](https://forums.foundationdb.org/c/site-feedback): A category for discussing the forums and the OSS project, its organization, how it works, and how we can improve it.

### Using GitHub Issues and Community Forums
GitHub issues and the community forums both offer features to facilitate discussion. To clarify how and when to use each tool, here's a quick summary of how we think about them:

GitHub Issues should be used for tracking tasks. If you know the specific code that needs to be changed, then it should go to GitHub Issues. Everything else should go to the Forums. For example: 

* I am experiencing some weird behavior, which I think is a bug, but I don't know where exactly (mysteries and unexpected behaviors): *Forums*
* I see a bug in this piece of code: *GitHub Issues*
* Feature requests and design documents: *Forums*
* Implementing an agreed upon feature: *GitHub Issues*

### Project and Development Updates
Stay connected to the project and the community! For project and community updates, follow the [FoundationDB project blog](https://www.foundationdb.org/blog/). Development announcements will be made via the community forums' [dev-announce](https://forums.foundationdb.org/c/development/dev-announce) section.
