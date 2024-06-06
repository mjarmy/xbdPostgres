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
    // Turn on auto-commit for new connections.
    c.autoCommit = true
  }

  protected override Void onClose(SqlConn c)
  {
    // Close the stashed prepared statements.
    c.stash.each |prep, sql|
    {
      ((Statement) prep).close
    }
  }
}
