//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using sql

**
** HavenPool is designed to work with Haven
**
const class HavenPool : SqlConnPool
{
  new make(|This|? f) : super(f) {}

  protected override Void onOpen(SqlConn c)
  {
    echo("HavenPool.onOpen $c")

    // Turn off auto-commit for new connections.
    c.autoCommit = false
  }

  protected override Void onClose(SqlConn c)
  {
    echo("HavenPool.onClose $c")

    // Close the stashed prepared statements.
    c.stash.each |v, proj|
    {
      smap := (Str:Statement) v
      smap.each |prep, sql|
      {
        ((Statement) prep).close
        echo("    closing $proj '$sql'")
      }
    }
  }
}
