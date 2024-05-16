//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Rec is a record in the postgres 'rec' table.
**
internal const class Rec
{
  **
  ** Only used for unit tests
  **
  internal new make(
    Ref id,
    Str[] paths,
    Str:Ref refs)
  {
    this.id       = id
    this.paths    = paths
    this.refs     = refs
  }

  **
  ** Only used for unit tests
  **
  override Int hash() { id.hash }

  **
  ** Only used for unit tests
  **
  override Bool equals(Obj? that)
  {
    x := that as Rec
    if (x == null) return false
    return (
      (id == x.id) &&
      (paths == x.paths) &&
      (refs == x.refs)
    )
  }

  override Str toStr() { "Rec($id)" }

  internal const Ref id
  internal const Str[] paths
  internal const Str:Ref refs
}
