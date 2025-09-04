
# Context

Intended for local testing of the alert-router.

Not very useful to test prod deployment etc since we have a deploy workflow for that. 

The real use case is if we want to either (1) quickly test the alert-router locally, or (2) test the EMAIL flow, since it includes `mailhog` which can validate the actual email contents.
