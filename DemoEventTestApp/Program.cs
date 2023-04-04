using DemoEventTestClient;

Console.WriteLine("----------------------------------------------------------------------------------");
Console.WriteLine("*                          TEST EVENT IN AVRO FORMAT                             *");
Console.WriteLine("----------------------------------------------------------------------------------");

DemoMessage message = new DemoMessage();
await message.SendTestEvent();
