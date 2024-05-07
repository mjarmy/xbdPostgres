//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   20 Mar 2024  Mike Jarmy  Creation
//

native class PostgresDb
{
  native Void open(Str uri, Str username, Str password)
  native Void close()

  native Void writeSpec(Str name, Str[] inherits)
  native Void writeRec(Str id, Str hayson, Arrow[] arrows)
  native Void writeFoo(Str id, Str hayson)
}
