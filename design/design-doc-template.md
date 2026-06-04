# Generic Software Design Doc Template

*Background: this is a template for software design documents.  To use it, make a copy and delete the italicized text.  The major sections below are those to include (or, occasionally, to skip — see the in-section guidance) in design docs.  The italicized text discusses rationale, what to include, and when a section can be omitted.*

*Some reasons for writing software design docs in general:*

1. *To confirm we are solving the right problem*
2. *To develop a thorough understanding of the problem*
3. *To develop a good understanding of the solution space*
4. *To identify areas where prototypes are needed to test approaches we cannot validate on paper*
5. *To identify problems in the design up front*
6. *To document our decision making for the benefit of future maintainers*

## Objective

*In one paragraph, clearly summarize the problem to be solved.*

*The scope of your document should be immediately clear to anyone who finds it; they shouldn't have to infer it from the title or from prior knowledge of what you are working on.*

## Background

*Provide background on the technical ecosystem and business context — typically one to several paragraphs.  It should be concise yet understandable to stakeholders beyond the immediate team, and does not have to be comprehensive (if important details are missing, reviewers will ask).*

*For some designs, a related discussion of Use Cases may be appropriate — particularly for new customer-facing features or subsystems of broad applicability that address several related end-user problems.  In that case, extending this section to multiple pages is fine.  (Note: the more contemporary term "CUJ" — Critical User Journey — is used essentially the same way as Use Case.)*

## Requirements

*What is required of the solution?  Be specific and explicit!*

*Writing requirements forces you to consider the problem and the class of potential solutions, not just your currently preferred solution.  This separation is very valuable in clarifying your thinking.*

*Requirements should be written so that it is easy to answer the question: Does the system actually meet the requirement?  Anything that can be specified in quantitative terms should be.*

*Sometimes requirements are more or less fully implied by the Objective, for designs of a very specific nature; in that case this section can be omitted.  Historically, however, Requirements is the single most overlooked section of design docs — it pays to take a step back and think them through in advance.*

*Typically the Requirements section, along with the Detailed Design, are the most important sections of the document.*

## Design Overview

*For more complicated designs, or designs that introduce many new concepts or components, it can be helpful to provide an overview.  Not all designs require this section.  If you use it, try to limit it to roughly half to one page, or it risks becoming redundant with the detailed design that follows.  Diagrams and lists of defined terms are often particularly useful here.*

## Detailed Design

*The design itself.  This should almost certainly be the longest section in the document.  Depending on length and complexity, it may benefit from subsections; their structure depends on the specifics of the design and is up to you.*

*Important things about this section:*

* *Is it coherent and understandable?*
* *Does the design fully address the problem?*
* *Is it clear how the requirements are met by the design?*
* *Beyond basic functionality, are considerations such as scalability, performance, and reliability/error handling discussed in appropriate detail?*

## Alternatives Considered

*There are usually multiple possible ways to solve a problem.  One of the reasons for writing a design doc is to think through the alternatives, pick the best one, and explain why it's best.  The detailed design above should focus on the chosen approach, not the alternatives (at least in a finalized doc); this section is where the rejected alternatives — along with their pros, cons, and the reasons for rejection — should be discussed explicitly.  This lets readers assess the thinking behind the design and flag problems.*

*Sometimes it is helpful to summarize pros/cons with a table where options are columns and decision criteria are rows.  In that case, put the table in the main Detailed Design section.*

## Testing Considerations

*How do you plan to test what you are building?  This section should discuss the general strategy; often multiple types of testing will be needed.  A comprehensive enumeration of all test cases is not the point here, but it's worth writing down a few representative test cases or scenarios.*

## Observability/Supportability Considerations

*How do you plan to support your design in production?  What metrics are needed to understand the operation of the system at a glance?  What SLO(s) might your system/feature offer?  What alerts are needed?  What complex scenarios deserve playbooks written and tested in advance?*

*This section can be removed if the design covers a non-production system.  For designs covering purely internal aspects of existing production systems already well covered by SLOs, playbooks, and observability, focus on whether any new metrics are needed — practically everything deserves some metrics on its operation.*

## Rollout/Migration Considerations

*How will your design be rolled out to production?  Are there considerations about migrating data?  Are rollbacks required, and if so, are there noteworthy considerations to think through in advance or test at some point?*

*This section can be deleted for designs covering non-production systems, or where the answer is obvious enough not to require discussion.*
