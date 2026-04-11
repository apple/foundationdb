# AI-generated FDB code overview

This directory contains an AI-generated code overview of the current `main` code base as of April, 2026.

This was generated using a commercially available coding assistant in widespread use.  We are not disclosing
the specific model used.

Prompts:

*Ok I want you to study the foundationdb code in the current
  directory.  It should be unmodified main.  I am not interested in
  specific diffs or PRs.  I am interested in understanding the entire
  system.  What are the major modules?  Do not rely too much on
  filenames.  Instead try to figure out the flow of data through the
  system and the dynamic relationships more than just the static
  arrangements.  I would like some assessment of the code in terms of
  a partition of subsystems that covers materially the entire code
  base, but not too granular.  I am thinking of 10-15 subsystems at
  most (or fewer if that is justified by the structure of the code).
  Tell me the major subsystems, the principle files in each, what they
  are responsible for, and so on.  Then we can do deep dives on each
  one.  But for now I want an overview of the whole system.*

This generated the file `foundationdb_subsystem_map.md`.

*save this exact full description to a file*

*for each of the 12 subsystems identified above, please study the code in depth and generate a module-specific architecture/design/implementation overview, focusing on the key data   
  structures, methods, and data flow in the system.  Please save each subsystem-specific overview to its own .md file

This generated the 12 files named `subsystem_*`.

## Rationale

Basically we did this on a Friday afternoon to see what the AI would
come up with.  On cursory examination it seems mostly reasonable.  We
hope this is usable as a starting point.  It is meant to supplement,
not replace, the existing documentation in the parent directory.

Note that the standard for developer-maintained documentation of
software systems is generally observable to be *widely uneven*.  Some
areas have in-depth, well-written deep dives.  Some areas are out of
date.  Some areas are not covered at all.  This is just the nature of
things.  So AI-generated documentation, by contrast, will be more
comprehensive in coverage, but not quite as in-depth.  It might have
random errors and omissions, but human-maintained documentation
certainly suffers from that too.

## Caveats

We have not reviewed this in depth, but plan to. Errors/omissions will
be incrementally addressed.  Feel free to send PRs to make
corrections.  At some point in a few quarters it may be appropriate to
completely regenerate this documentation with more advanced models.
