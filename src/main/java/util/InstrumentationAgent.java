package util;

public class InstrumentationAgent {

   /* private static volatile Instrumentation globalInstrumentation;

    public static void premain(final String agentArgs, final Instrumentation inst) {
        globalInstrumentation = inst;
    }

    public static long getObjectSize(final Object object) {
        if (globalInstrumentation == null) {
            throw new IllegalStateException("Agent not initialized.");
        }
        return globalInstrumentation.getObjectSize(object);

    }




    private class Measurer {

        public static void main(String[] args) {
            Set<Integer> hashset = new HashSet<Integer>();
            Random random = new Random();
            int n = 10000;
            for (int i = 1; i <= n; i++) {
                hashset.add(random.nextInt());
            }
            System.out.println(ObjectGraphMeasurer.measure(hashset));
        }*/
    }
