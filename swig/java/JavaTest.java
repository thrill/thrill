
import thrill.*;

class MyGen implements GeneratorFunction {
    public String call(long i) {
        return Long.toString(i) + "from java";
    }
}

public class JavaTest {
    public static void main(String argv[]) {
        thrill.printHello(42, new MyGen());
    }
}
