class Gene[T](allele: T) {
  val Allele: T = allele
  def Mutate[T](newAllele: T) = new Gene(newAllele)
}
