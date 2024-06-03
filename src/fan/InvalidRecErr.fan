//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   3 June 2024  Mike Jarmy  Creation
//

**
** InvalidRecErr
**
internal const class InvalidRecErr : Err {
  new make(Str msg, Err? cause := null) : super(msg, cause) {}
}
