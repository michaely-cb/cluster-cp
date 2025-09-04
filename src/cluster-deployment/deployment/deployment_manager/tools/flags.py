import argparse


def add_image_flag(parser: argparse.ArgumentParser, required=True) -> argparse.ArgumentParser:
    parser.add_argument(
        "--image",
        help=str(
            "CS image file. On monolith, the image is typically "
            "/cb/artifacts/releases/Cerebras_CS1-1.8.0/components/itb-sm/CS1-<version>-<build_id>.tar.gz",
        ),
        type=str,
        required=required
    )
    return parser


def add_system_user_pw_flags(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--username",
        help="CS system username",
        type=str,
        required=False,
        default='admin',
    )
    parser.add_argument(
        "--password",
        help="CS system password",
        type=str,
        required=False,
        default='admin',
    )
    return parser
