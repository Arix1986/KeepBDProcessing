package keepcod

object excercies {
  
  def last[A] (lista:List[A]): Unit={
       if(lista.isEmpty){
           println("Error: Necesitamos una Lista con n Elementos")
       }
     
      else {
           println(s"${lista.last}")
      }
    
  }

  def penUlt[A](lista:List[A]): Unit={
    if(lista.length >= 2){
        println(s"${lista(lista.size - 2 )}")
    }
    else {
            println("Error: Necesitamos una Lista con mas de 2 Elementos")
      }
  }

  def reverseFunctional[A](ls: List[A]): List[A] = ls.foldLeft(List[A]()) { (r, h) => h :: r}

  def sumarRev[A](ls: List[Int]): Int = ls.foldLeft(0) { (r, h) => h + r}
  
  def delduplicate[A](lista:List[A]):List[A]={
       lista match{
        case Nil => Nil
        case h :: t =>{
            h :: delduplicate(t.dropWhile(_==h))
        } 

       }
  }

  def anotherdupli[A](lista:List[A]):List[A]= lista.foldLeft(List[A]()) {(acc,h) => if(!acc.contains(h)) acc :+ h else acc  }
  
  def reclast[A](ls:List[A]):Option[A]={
    ls match{
        case Nil => None
        case h :: Nil => Some(h)
        case h :: r =>{
            val last = r(r.size -1)
            if(last == h) Some(h) else reclast(r)
        }
    }
  }

  def recNumeroElementos[A](ls:List[A],acc: Int=0):Int={
    ls match{
        case Nil => acc
        case _ :: r => recNumeroElementos(r,acc + 1)
       
    }
  }
  def recInvertirList[B](ls:List[B]):List[B]={
    ls match {
        case Nil => Nil
        case h :: r => recInvertirList(r) :+ h
    }
  }
 
  def RecCod[A](ls:List[A], lf:List[(A, Int)]=List()):List[(A, Int)]={

    ls match{
        case Nil =>lf
        case h :: r => {
           val lisfilter =  r.filter(n=> n == h)
           val cant=lisfilter.length + 1
           RecCod(r.filter(_ != h), lf :+ (h,cant)) 
        }
    }
  }

  def RecCod2[A](ls:List[A], lf:List[(A, Int)]=List()):List[(A, Int)]={
     ls match{
        case Nil =>lf
        case h :: r => {
           val cant = r.count(_ == h) + 1
           RecCod(r.filter(_ != h), lf :+ (h,cant)) 
        }
    }
  }


}
