import collections
import collections.abc
import functools
import warnings
from urllib.parse import parse_qs, urlparse

import httpx

from ..utils import import_object, prepend_to_sys_path
from .container import DEFAULT_STRUCTURE_CLIENT_DISPATCH, Container
from .context import DEFAULT_TIMEOUT_PARAMS, UNSET, Context, send_requests, send_requests_async, requestor
from .utils import MSGPACK_MIME_TYPE, client_for_item, handle_error, retry_context


def from_uri(
    uri,
    structure_clients="numpy",
    *,
    cache=UNSET,
    remember_me=True,
    username=None,
    auth_provider=None,
    api_key=None,
    verify=True,
    prompt_for_reauthentication=None,
    headers=None,
    timeout=None,
    include_data_sources=False,
    awaitable=False,
):
    """
    Connect to a Node on a local or remote server.

    Parameters
    ----------
    uri : str
        e.g. "http://localhost:8000"
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping a
        structure_family or a spec to a client object.
    cache : Cache, optional
    remember_me : bool
        Next time, try to automatically authenticate using this session.
    username : str, optional
        DEPRECATED. Ignored, and issues a warning if passed.
    auth_provider : str, optional
        DEPRECATED. Ignored, and issues a warning if passed.
    api_key : str, optional
        API key based authentication.
    verify : bool, optional
        Verify SSL certifications. True by default. False is insecure,
        intended for development and testing only.
    prompt_for_reauthentication : bool, optional
        DEPRECATED. Ignored, and issue a warning if passed.
    headers : dict, optional
        Extra HTTP headers.
    timeout : httpx.Timeout, optional
        If None, use Tiled default settings.
        (To disable timeouts, use httpx.Timeout(None)).
    include_data_sources : bool, optional
        Default False. If True, fetch information about underlying data sources.
    """
    EXPLAIN_LOGIN = """

The user will be prompted for credentials if login is required.
Or, call login() to manually login.

For non-interactive authentication, use an API key.
"""
    if username is not None:
        warnings.warn("Tiled no longer accepts 'username' parameter. " + EXPLAIN_LOGIN)
    if auth_provider is not None:
        warnings.warn(
            "Tiled no longer accepts 'auth_provider' parameter. " + EXPLAIN_LOGIN
        )
    if auth_provider is not None:
        warnings.warn(
            "Tiled no longer accepts 'prompt_for_reauthentication' parameter. "
            + EXPLAIN_LOGIN
        )
    context, node_path_parts = yield from Context.from_any_uri.__wrapped__(
        Context,
        uri,
        api_key=api_key,
        cache=cache,
        headers=headers,
        timeout=timeout,
        verify=verify,
        awaitable=awaitable,
    )
    yield from context.connect()
    return (yield from build_from_context(
        context,
        structure_clients=structure_clients,
        node_path_parts=node_path_parts,
        include_data_sources=include_data_sources,
        remember_me=remember_me,
        awaitable=awaitable,
    ))


def from_context(
    context: Context,
    structure_clients="numpy",
    node_path_parts=None,
    include_data_sources=False,
    remember_me=True,
    awaitable: bool = False,
):
    """
    Advanced: Connect to a Node using a custom instance of httpx.Client or httpx.AsyncClient.

    Parameters
    ----------
    context : tiled.client.context.Context
    structure_clients : str or dict, optional
        Use "dask" for delayed data loading and "numpy" for immediate
        in-memory structures (e.g. normal numpy arrays, pandas
        DataFrames). For advanced use, provide dict mapping a
        structure_family or a spec to a client object.
    """
    node_path_parts = node_path_parts or []
    # Do entrypoint discovery if it hasn't yet been done.
    if Container.STRUCTURE_CLIENTS_FROM_ENTRYPOINTS is None:
        Container.discover_clients_from_entrypoints()
    # Interpret structure_clients="numpy" and structure_clients="dask" shortcuts.
    if isinstance(structure_clients, str):
        struct_clients_key = structure_clients + "_async" if awaitable else structure_clients
        structure_clients = DEFAULT_STRUCTURE_CLIENT_DISPATCH[struct_clients_key]
    # To construct a user-facing client object, we may be required to authenticate.
    # 1. If any API key set, we are already authenticated and there is nothing to do.
    # 2. If there are cached valid credentials for this server, use them.
    # 3. If not, and the server requires authentication, prompt for authentication.
    if context.api_key is None:
        auth_is_required = context.server_info.authentication.required
        has_providers = len(context.server_info.authentication.providers) > 0
        if auth_is_required and not has_providers:
            raise RuntimeError(
                """This server requires API key authentication.
    Set an api_key as in:

    >>> c = from_uri("...", api_key="...")
    """
            )
        if has_providers:
            found_valid_tokens = remember_me and context.use_cached_tokens()
            if (not found_valid_tokens) and auth_is_required:
                # Bundle the request with the context so other constructors have it
                yield from context.authenticate.__wrapped__(context, remember_me=remember_me)
    # Context ensures that context.api_uri has a trailing slash.
    item_uri = f"{context.api_uri}metadata/{'/'.join(node_path_parts)}"
    params = parse_qs(urlparse(item_uri).query)
    if include_data_sources:
        params["include_data_sources"] = include_data_sources
    content = (
        yield context.build_request(
            "GET",
            item_uri,
            headers={"Accept": MSGPACK_MIME_TYPE},
        )
    ).json()
    item = content["data"]
    return client_for_item(
        context, structure_clients, item, include_data_sources=include_data_sources
    )


def from_profile(name, structure_clients=None, **kwargs):
    """
    Build a Node based a 'profile' (a named configuration).

    List available profiles and the source filepaths from Python like:

    >>> from tiled.profiles import list_profiles
    >>> list_profiles()

    or from a CLI like:

    $ tiled profile list

    Or show the file contents like:

    >>> from tiled.profiles import load_profiles
    >>> load_profiles()

    or from a CLI like:

    $ tiled profile show PROFILE_NAME

    Any additional parameters override profile content. See from_uri for details.
    """
    # We accept structure_clients as a separate parameter so that it
    # may be invoked positionally, as in from_profile("...", "dask").
    from ..profiles import ProfileNotFound, load_profiles, paths

    profiles = load_profiles()
    try:
        filepath, profile_content = profiles[name]
    except KeyError as err:
        raise ProfileNotFound(
            f"Profile {name!r} not found. Found profiles {list(profiles)} "
            f"from directories {paths}."
        ) from err
    merged = {**profile_content, **kwargs}
    if structure_clients is not None:
        merged["structure_clients"] = structure_clients
    cache_config = merged.pop("cache", None)
    if cache_config is not None:
        from tiled.client.cache import Cache

        if isinstance(cache_config, collections.abc.Mapping):
            # All necessary validation has already been performed
            # in load_profiles().
            cache = Cache(**cache_config)
        else:
            # Interpret this as a Cache object passed in directly.
            cache = cache_config
        merged["cache"] = cache
    timeout_config = merged.pop("timeout", None)
    if timeout_config is not None:
        if isinstance(timeout_config, httpx.Timeout):
            timeout = timeout_config
        else:
            timeout_params = DEFAULT_TIMEOUT_PARAMS.copy()
            timeout_params.update(timeout_config)
            timeout = httpx.Timeout(**timeout_params)
        merged["timeout"] = timeout
    # Below, we may convert importable strings like
    # "package.module:obj" to live objects. Include the profile's
    # source directory in the import path, temporarily.
    with prepend_to_sys_path(filepath.parent):
        structure_clients_ = merged.pop("structure_clients", None)
        if structure_clients_ is not None:
            if isinstance(structure_clients_, str):
                # Nothing to do.
                merged["structure_clients"] = structure_clients_
            else:
                # This is a dict mapping structure families like "array" and "dataframe"
                # to values. The values may be client objects or importable strings.
                result = {}
                for key, value in structure_clients_.items():
                    if isinstance(value, str):
                        class_ = import_object(value, accept_live_object=True)
                    else:
                        class_ = value
                    result[key] = class_
                merged["structure_clients"] = result
    if "direct" in merged:
        # The profile specifies the server in-line.
        # Create an app and use it directly via ASGI.
        import jsonschema

        from ..config import ConfigError, schema
        from ..server.app import build_app_from_config

        config = merged.pop("direct", None)
        try:
            jsonschema.validate(instance=config, schema=schema())
        except jsonschema.ValidationError as err:
            msg = err.args[0]
            raise ConfigError(
                f"ValidationError while parsing configuration file {filepath}: {msg}"
            ) from err
        context = yield from Context.from_app.__wrapped__(
            Context, build_app_from_config(config, source_filepath=filepath),
        )
        return (yield from build_from_context(context, **merged))
    else:
        return (yield from build_from_uri(**merged))


# Stash the builder functions, and replace them with decorated functions
build_from_context = from_context
build_from_uri = from_uri
build_from_profile = from_profile

from_context_async = functools.partial(from_context, awaitable=True)
from_context_async = requestor(asynchronous=True)(from_context_async)
from_uri_async = functools.partial(from_uri, awaitable=True)
from_uri_async = requestor(asynchronous=True)(from_uri_async)
from_profile_async = functools.partial(from_profile, awaitable=True)
from_profile_async = requestor(asynchronous=True)(from_profile_async)

from_context = requestor(asynchronous=False)(from_context)
from_uri = requestor(asynchronous=False)(from_uri)
from_profile = requestor(asynchronous=False)(from_profile)
