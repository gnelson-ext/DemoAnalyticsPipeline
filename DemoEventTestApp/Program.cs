using DemoEventTestClient;

Console.WriteLine("----------------------------------------------------------------------------------");
Console.WriteLine("*                          TEST EVENT IN JSON FORMAT                             *");
Console.WriteLine("----------------------------------------------------------------------------------");

DemoMessage message = new DemoMessage();
await message.SendTestEvent();
Console.WriteLine(Environment.NewLine);
