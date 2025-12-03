
import java.util.Random;

public class PowerLawGraph {
    public static int[] generateDegrees(int numNodes) {
        double TAU = 2.0;
        double C = (2.0 * (TAU - 1.0)) / (1.0 + TAU);
        double[] cumulativeProb = new double[numNodes];
        double sumProb = 0.0;

        for (int k = 1; k < numNodes; k++) {
            double pk = C * Math.pow(k, -TAU);
            cumulativeProb[k] = pk;
            sumProb += pk;
        }

        for (int k = 1; k < numNodes - 1; k++) {
            cumulativeProb[k] /= sumProb;
            cumulativeProb[k] += cumulativeProb[k - 1];
        }

        int[] freq = new int[numNodes];
        Random rand = new Random();
        for (int i = 0; i < numNodes; i++) {
            double r = rand.nextDouble();
            for (int j = 1; j < numNodes; j++) {
                if (r <= cumulativeProb[j]) {
                    freq[j]++;
                    break;
                }
            }
        }

        int[] degrees = new int[numNodes];
        int index = 0;
        for (int k = numNodes - 1; k >= 1; k--) {
            for (int count = 0; count < freq[k]; count++) {
                if (index < numNodes) {
                    degrees[index] = k;
                    index++;
                }
            }
        }

        return degrees;
    }
}
