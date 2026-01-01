import contextlib
import copy
import dataclasses
import importlib
import importlib.metadata
import os
import re
import sqlite3
import subprocess
import sys
import urllib
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Optional

from .fuse import fuse


def parse_requirement(requirement: str) -> Optional[tuple[str, list[str], Optional[str]]]:
    # https://packaging.python.org/en/latest/specifications/name-normalization/
    # Match only valid project name and avoid cruft like extras and requirements, e.g.,
    # indexed_gzip >= 1.6.3, != 1.9.4; python_version < '3.8'
    match = re.match(
        r"^([A-Za-z0-9]([^A-Za-z0-9]|$)|[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])(\[([A-Za-z0-9, ]+)\])?"
        r"""(.*;.* extra ?== ?["']([^"']+)["'])?""",
        requirement,
    )
    if not match:
        return None
    # print(requirement," -> GROUPS:", [match.group(i) for i in range(1 + len(match.groups()))])
    return match.group(1), match.group(4).split(',') if match.group(4) else [], match.group(6)


def get_readme(name: str) -> str:
    try:
        return str(importlib.metadata.metadata(name)["Description"])
    except Exception as exception:
        return f"Error loading README: {exception}"


def print_metadata_recursively(
    packages: dict[str, set[str]],
    doWithDistribution: Callable[[Any], None],
    doOnNewLevel: Optional[Callable[[int], None]] = None,
    level: int = 0,
    processedPackages: Optional[set[str]] = None,
):
    if processedPackages is None:
        processedPackages = set()

    if not any(package not in processedPackages for package, _ in packages.items()):
        return
    if doOnNewLevel:
        doOnNewLevel(level)

    requirements: dict[str, set[str]] = {}
    for package, enabledExtras in sorted(packages.items()):
        # For now, packages specified with different extra will result in the latter being omitted.
        if package in processedPackages:
            continue
        processedPackages.add(package)

        try:
            distribution = importlib.metadata.distribution(package)
        except importlib.metadata.PackageNotFoundError:
            # Will happen for uninstalled optional or Python version dependent files and built-in modules, such as:
            #   fastzipfile, argparse, ...
            continue

        doWithDistribution(distribution)

        for requirement in distribution.requires or []:
            parsed = parse_requirement(requirement)
            if not parsed:
                # Should not happen and does not in my tests.
                print(f"  Cannot parse requirement: {requirement}")
                continue

            requiredPackage, packageExtras, extraNamespace = parsed
            if "full" not in enabledExtras and extraNamespace is not None and extraNamespace not in enabledExtras:
                continue
            if requiredPackage in processedPackages:
                continue

            requirements[requiredPackage] = requirements.get(requiredPackage, set()).union(set(packageExtras))

    print_metadata_recursively(requirements, doWithDistribution, doOnNewLevel, level + 1, processedPackages)


@dataclasses.dataclass
class VersionInformation:
    name: str = ''
    version: str = ''
    authors: list[str] = dataclasses.field(default_factory=list)
    urls: list[str] = dataclasses.field(default_factory=list)
    summary: str = ''
    licenses: list[str] = dataclasses.field(default_factory=list)
    license_short: str = ''


NON_PYTHON_LIB_LICENSES = {
    "libfuse": ("/libfuse/libfuse/refs/heads/master/LGPL2.txt", "LGPL-2.1"),
    # "The author disclaims copyright to this source code"
    "libsqlite3": ("/sqlite/sqlite/master/LICENSE.md", "None"),
    "cpython": ("/python/cpython/main/LICENSE", "PSF-2.0"),  # PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
    "libzstd-seek": ("/martinellimarco/libzstd-seek/main/LICENSE", "MIT"),
    "libzstd": ("/facebook/zstd/dev/LICENSE", "BSD-3"),  # BSD-3 with "name of the copyright holder" filled in
    "libz": ("/madler/zlib/refs/heads/master/LICENSE", "zlib License"),
    # BSD-3 with "name of the copyright holder" explicitly filled in
    "sqlcipher": ("/sqlcipher/sqlcipher/refs/heads/master/LICENSE.txt", "BSD-3"),
    "python-ext4": ("/Eeems/python-ext4/refs/heads/main/LICENSE", "MIT"),
}


def get_url_and_license_from_github_subpath(subpath: str) -> tuple[str, str]:
    licenseUrl = "https://raw.githubusercontent.com" + subpath
    try:
        licenseContents = urllib.request.urlopen(licenseUrl, timeout=3).read().decode()
    except urllib.error.URLError as error:
        licenseContents = f"Failed to get license at {licenseUrl} because of: {error!s}"
    url = "https://github.com" + '/'.join(subpath.split('/', 3)[:3])
    return url, licenseContents


def gather_system_software_versions(with_licenses: bool = True) -> list[tuple[str, VersionInformation]]:
    non_python_libs = [
        (sys.implementation.name, '.'.join(str(i) for i in sys.implementation.version[:3])),
        ("libsqlite3", sqlite3.sqlite_version),
    ]
    if hasattr(fuse, 'fuse_version_major') and hasattr(fuse, 'fuse_version_minor'):
        non_python_libs.append(("libfuse", f"{fuse.fuse_version_major}.{fuse.fuse_version_minor}"))

    system_software: list[tuple[str, VersionInformation]] = []
    for name, version in non_python_libs:
        url = ''
        license_contents = ''
        license_short = ''
        if name in NON_PYTHON_LIB_LICENSES:
            subpath, license_short = NON_PYTHON_LIB_LICENSES[name]
            if with_licenses:
                url, license_contents = get_url_and_license_from_github_subpath(subpath)
        system_software.append(
            (
                name,
                VersionInformation(
                    name=sys.implementation.name,
                    version=version,
                    urls=[url],
                    licenses=[license_contents],
                    license_short=license_short,
                ),
            )
        )

    try:
        fusermountVersion = subprocess.run(["fusermount", "--version"], capture_output=True, check=False).stdout.strip()
        system_software.append(
            (
                "fusermount",
                VersionInformation(
                    name="fusermount", version=re.sub('.* ([0-9][.][0-9.]+).*', r'\1', fusermountVersion.decode())
                ),
            )
        )
    except Exception:
        pass

    return system_software


def gather_shared_library_versions() -> list[tuple[str, VersionInformation]]:
    mappedFilesFolder = f"/proc/{os.getpid()}/map_files"
    if os.path.isdir(mappedFilesFolder):
        libraries = {os.readlink(os.path.join(mappedFilesFolder, link)) for link in os.listdir(mappedFilesFolder)}
        # Only look for shared libraries with versioning suffixed. Ignore all ending on .so.
        libraries = {library for library in libraries if '.so.' in library}
        if libraries:
            result = []
            for library in sorted(libraries):
                name, version = library.rsplit('/', maxsplit=1)[-1].split('.so.', maxsplit=1)
                result.append((name, VersionInformation(name=name, version=version)))
            return result
    return []


def gather_version_information(distribution, with_licenses: bool = True) -> VersionInformation:
    name = distribution.metadata.get('Name', '')

    # Analyze LICENSE file.
    licenses: list[str] = []
    if with_licenses:
        for key, value in distribution.metadata.items():
            if key == 'License-File':
                # All system-installed packages do not seem to be distributed with a license:
                # find /usr/lib/python3/dist-packages/ -iname '*license*'
                licenseContents = distribution.read_text(value) or distribution.read_text(
                    os.path.join("licenses", value)
                )
                if licenseContents:
                    licenses.append(licenseContents)
                    continue

        # This is known to happen for system-installed packages :/, and --editable installed packages.
        if not licenses:
            path = Path(f"/usr/share/doc/python3-{name}/copyright")
            if path.is_file():
                licenses.append(path.read_text(encoding='utf-8'))

    return VersionInformation(
        name=name,
        version=str(distribution.version),
        authors=[x for key, x in distribution.metadata.items() if key == 'Author' and x],
        urls=[x for key, x in distribution.metadata.items() if key == 'Project-URL' and x],
        summary=distribution.metadata.get('Summary', ''),
        licenses=licenses,
        license_short=find_short_license(distribution),
    )


def gather_versions(with_licenses: bool = True) -> dict[Any, list[tuple[str, VersionInformation]]]:
    gathered: list[tuple] = []

    def do_for_distribution(distribution):
        if 'Name' not in distribution.metadata:
            return

        version_info = gather_version_information(distribution, with_licenses=with_licenses)
        gathered.append((version_info.name, version_info))

        # Import the module in order to open the shared libraries so that we can look for loaded shared
        # libraries and list their versions!
        topLevel = distribution.read_text('top_level.txt')
        if topLevel:
            for module in topLevel.strip().split('\n'):
                # dropboxdrivefs installs a module named "test" and unicrypto a module named "tests".
                # This seems like a packaging bug to me because the names are too broad and the modules useless.
                if module and not module.startswith('_') and not module.startswith('test'):
                    with contextlib.suppress(Exception):
                        importlib.import_module(module)

    versions_by_depth = {}

    def process_new_level(level):
        if not gathered:
            return

        versions_by_depth[level] = copy.deepcopy(gathered)
        gathered.clear()

    print_metadata_recursively({"ratarmount": {"full"}}, do_for_distribution, process_new_level)

    if gathered:
        versions_by_depth[max(versions_by_depth.keys()) + 1] = copy.deepcopy(gathered)
    versions_by_depth["System Software"] = gather_system_software_versions(with_licenses=with_licenses)
    versions_by_depth["Versioned Loaded Shared Libraries"] = gather_shared_library_versions()
    return versions_by_depth


def print_versions() -> None:
    for label, versions in gather_versions(with_licenses=False).items():
        if not versions:
            continue

        if isinstance(label, int):
            if label > 1:
                print(f"\nLevel {label} Dependencies:\n")
        else:
            print(f"\n{label}:\n")

        for name, distribution in versions:
            print(f"{name} {distribution.version}")


def find_short_license(distribution) -> str:
    shortLicense = ""

    # Check classifiers
    for key, value in distribution.metadata.items():
        if key == "Classifier" and value.startswith("License ::") and not value.endswith(":: OSI Approved"):
            if shortLicense:
                shortLicense += " OR "
            shortLicense += re.sub(r"([A-Z]+) License", r"\1", value.rsplit("::", 1)[-1].strip())

    # webdav4 only has this License-Expression.
    if not shortLicense:
        for key, value in distribution.metadata.items():
            if key == "License-Expression":
                shortLicense += value
                break

    # Check LICENSE key.
    if not shortLicense and 'LICENSE' in distribution.metadata and '\n' not in distribution.metadata['LICENSE']:
        shortLicense = distribution.metadata['LICENSE']

    # Analyze LICENSE file.
    if not shortLicense and 'License-File' in distribution.metadata:
        licenseContents = distribution.read_text(distribution.metadata['License-File'])
        if licenseContents:
            matched = re.match(r"^((MIT|BSD|GPL|LGPL).*)( License)?", licenseContents.split('\n')[0])
            if matched:
                shortLicense = matched.group(2)

    if shortLicense == "GNU Lesser General Public License v2 or later (LGPLv2+)":
        shortLicense = "LGPLv2+"
    elif shortLicense == "GNU Library or Lesser General Public License (LGPL)":
        shortLicense = "LGPL"

    return shortLicense


def create_oss_markdown(version_info: VersionInformation) -> str:
    result = f"# {version_info.name}\n\n"
    if version_info.summary:
        result += version_info.summary + "\n\n"

    if version_info.urls:
        for url in version_info.urls:
            split = url.split(', ', maxsplit=1)
            name = 'Homepage'
            if len(split) == 2:
                name = split[0]
            result += f"[{name}]({split[-1]})\n\n"
        result += "\n"

    if version_info.authors:
        result += f"Authors: {', '.join(version_info.authors)}\n\n"

    if version_info.licenses:
        for licenseContents in version_info.licenses:
            result += "```\n" + licenseContents.strip('\n') + "\n```\n\n"
    else:
        result += f"{version_info.name} License: {version_info.license_short}"

    return result


def print_oss_attributions(short: bool = False, with_licenses: bool = False) -> None:
    def do_for_distribution(distribution):
        if 'Name' not in distribution.metadata:
            return

        version_info = gather_version_information(distribution, with_licenses=with_licenses)
        if short:
            print(f"{version_info.name:20} {distribution.version:12} {version_info.license_short}")
            return

        print(create_oss_markdown(version_info) + "\n")

    def print_on_new_level(level):
        if level > 1:
            print(f"\nLevel {level} Dependencies:\n")

    print_metadata_recursively({"ratarmount": {"full"}}, do_for_distribution, print_on_new_level if short else None)

    print("\nSystem Software:\n")

    system_software_versions = gather_system_software_versions(with_licenses=with_licenses)
    shared_library_versions = gather_shared_library_versions()
    for name, values in sorted(NON_PYTHON_LIB_LICENSES.items()):
        githubPath, shortLicense = values
        version = ''
        for other_name, info in system_software_versions + shared_library_versions:
            if name == other_name:
                version = str(info.version)
        if short:
            # {version:12} in the middle is missing because I don't have this information here.
            print(f"{name:20} {version:12} {shortLicense}")
            continue
        if with_licenses:
            homepage, licenseContents = get_url_and_license_from_github_subpath(githubPath)
            print(f"# {name}\n\n{homepage}\n\n\n```\n{licenseContents}\n```\n\n")
