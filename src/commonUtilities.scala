object Args{
    def toMap(args: Array[String]): Map[String,String]={
        val argMap : Map[String,String] = args.map(_.split('=')).collect{
            case Array(key, value) => key -> value
        }.toMap
        return argMap
    }

    def getValue(key: String, argMap: Map[String,String] ): String={
        argMap.getOrElse(key, throw new KeyNotFoundException(s"Key '$key' not passed as an argument"))
    }
}

class KeyNotFoundException(s: String)  extends Exception(s)