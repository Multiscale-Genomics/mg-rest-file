Architectural Decision Record (ADR)
===================================

This file is a record of the choices that have been made about the choice of
software, packages, pipelines and data structures that have been made in this
repository. This document should serve the help future developers (including the
original authors) understand what certain choices were made.


.. dd-mm-yyy - Title
   -----------------

   Description of change


21-09-2017 - Split off file retrieval from the DM RESTful Server
----------------------------------------------------------------

This was done as this is a separate behaviour from the management of the metadata
and tracking of files.