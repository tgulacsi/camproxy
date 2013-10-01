# Camproxy - simplifier proxy for Camlistore #
To be able to upload and download simple files, without local camput/camget,
you can start a camproxy on a machine with camput and camget (camput is needed,
camget is optional).

## Rationale ##
I have a legacy AIX 5.3 system, without go - thus camlistore is not running it.
But I love Camlistore's deduplication and versatility of backends.

### Warning
As my primary usage is uploading and downloading *single* files, and have only
some vague idea about handling directories/static-sets (download as tar/zip),
this usage is absolutely unhandled yet.

## Usage ##
    CAMLI_AUTH=userpass:login:passwd camproxy [-server=https://camli.myserver.net] [-listen=:3128]
Thus
    CAMLI_AUTH=userpass:login:passwd camproxy -listen=0.0.0.0:3128
will start a proxy listening on `0.0.0.0:3128`, putting/getting to/from
the camlistore server on `localhost:3147`, using the default gnupg secret keyring
(`$HOME/.config/camlistore/identity-secring.gpg`) with the default configuration
(`$HOME/.config/camlistore/client-config.json`).

### Upload ###
This means that upload is a simple
    curl -F upfile=@filenametoupload http://camproxy.host:3148

Which returns the blobref (i.e. "sha1-c4991c8f57d2639f779c53c12392369ed81d426").
If you set permanode=1, than two refs are returned, first is the file ref,
second is the permanode's ref:
    curl -F upfile=@filenametoupload http://camproxy.host:3138?permanode=1
returns
    sha1-c4276dae3345bd92a4616b7688d800774d6abbeb
    sha1-c11e44e38201eb830379327824527f559315de79

The permanode is different for each upload, of course; but the file's ref is
different, too - this is only because the uploaded file's metadata
(mtime, for example) is different for each upload. This can be alleviated
by sending the file mtime, too. Just send the mtime=<seconds since epoch>
parameter. But this is just cosmetic, as the content of the file is stored
only once - see below.

For my use space is scarce, thus if you set the `short=1` param, then a
base64-encoded blob ref (34 chars) is returned instead of the official
hex-encoded (45 chars) one.

### Download ###
    curl http://camproxy.host:3148/sha1-c4276dae3345bd92a4616b7688d800774d6abbeb
Will return the file's content.
    curl http://camproxy.host:3148/sha1-c4276dae3345bd92a4616b7688d800774d6abbeb?raw=1
returns the blob (json) as stored in Camlistore.

The short, bas64-encoded (sha1-toJZZKCSCnNBWuJrT3JH-3qIZbU=) is accepted, too.

