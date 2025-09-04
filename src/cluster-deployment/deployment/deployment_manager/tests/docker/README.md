# Deployment Manager Integration Test Docker

## Caveats

There are notable gaps between the docker image and the real testing environment
- Docker image is not the exact OS version we're running at time of writing (8.5 versus 8.9).
- Set up steps are different than `os-build`

However, that's not enough downsides to not get value from the tests.

## Updating the Image

If you update the docker image, also bump the tag in the `Makefile` and do a find/replace
on the codebase for the old tag. Then do a `make push` to upload the new image to ECR.
