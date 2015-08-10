
import c7a.*;

class MyGen implements GeneratorFunction {
    public String call(long i) {
        return Long.toString(i) + "from java";
    }
}

public class JavaTest {
    public static void main(String argv[]) {
        c7a.printHello(42, new MyGen());
    }
}
