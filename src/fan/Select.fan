//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Select
**
const class Select
{
  new make(Str sql, Obj[] params)
  {
    this.sql = "foo"
    this.params = [,]
  }

  const Str sql
  const Obj[] params

  override Int hash() { sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Select
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Select('$sql', $params)" }
}
