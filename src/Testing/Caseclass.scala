package Testing

// 1st Case create case class and print .

case class Message(sender: String, recipient: String, body: String)

object Caseclass {

  def main(args: Array[String]) {

    val message1 = Message("abc@gmail.com", "cde@catalonia", "Ça va ?")

    println(message1.sender) // prints abc@gmail.com
    println(message1.recipient)
    println(message1.body)
    // message1.sender = "travis@washington.us"  // this line does not compile

    /*  2nd Case class Comparison .
************************** Case class Comparison*********************************/

    val message2 = Message("abc@gmail.com", "cde@catalonia", "Ça va ?")
    val messageComparison = message1 == message2
    println(messageComparison)

    /*    3rd Case class Copying .
************************** Case class Copying*********************************/
    
    val message3 = message2.copy(sender="123abc@gmail.com", recipient=message2.sender)
   println("Testing:  "+message3)
   
  }

}