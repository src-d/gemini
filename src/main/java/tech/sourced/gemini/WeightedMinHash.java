package tech.sourced.gemini;

import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static java.lang.Math.floor;
import static java.lang.Math.log;

/**
 * Weighted MinHash implementation based on
 * https://github.com/ekzhu/datasketch/blob/master/datasketch/weighted_minhash.py
 * https://github.com/src-d/go-license-detector/blob/master/licensedb/internal/wmh/wmh.go
 */
public class WeightedMinHash implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(WeightedMinHash.class);

    protected int dim;
    protected int sampleSize;

    protected float[][] rs;
    protected float[][] lnCs;
    protected float[][] betas;

    /**
     * Initializes a WeightedMinHash
     *
     * @param dim the number of dimensions in the input. i.e. size of the weight vector to hash
     */
    public WeightedMinHash(int dim) {
        this(dim, 128, 1);
    }

    /**
     * Initializes a WeightedMinHash
     *
     * @param dim number of dimensions in the input. i.e. size of the weight vector to hash
     * @param sampleSize number of samples to use to initialize the weighted MinHash
     * @param seed random generator seed for reproducible results
     */
    public WeightedMinHash(int dim, int sampleSize, long seed) {
        this.dim = dim;
        this.sampleSize = sampleSize;

        // Well19937c is the default, MersenneTwister is the one used by python
        //RandomGenerator randSrc = new Well19937c(seed);
        RandomGenerator randSrc = new MersenneTwister(seed);

        GammaDistribution gammaGen = new GammaDistribution(randSrc, 2, 1);

        rs = new float[sampleSize][dim];

        for (int y = 0; y < sampleSize; y++) {
            float[] arr = rs[y];
            for (int x = 0; x < dim; x++) {
                arr[x] = (float)gammaGen.sample();
            }
        }

        lnCs = new float[sampleSize][dim];

        for (int y = 0; y < sampleSize; y++) {
            float[] arr = lnCs[y];
            for (int x = 0; x < dim; x++) {
                arr[x] = (float)log(gammaGen.sample());
            }
        }

        UniformRealDistribution uniformGen = new UniformRealDistribution(randSrc, 0, 1);

        betas = new float[sampleSize][dim];

        for (int y = 0; y < sampleSize; y++) {
            float[] arr = betas[y];
            for (int x = 0; x < dim; x++) {
                arr[x] = (float)uniformGen.sample();
            }
        }
    }

    WeightedMinHash(int dim, int sampleSize, float[][] rs, float[][] lnCs, float[][] betas) {
        this.dim = dim;
        this.sampleSize = sampleSize;
        this.rs = rs;
        this.lnCs = lnCs;
        this.betas = betas;
    }

    /**
     * Calculates the weighted MinHash for the given weighted vector
     *
     * @param values weighted vector
     * @return weighted MinHash
     */
    public long[][] hash(float[] values) {
        if (values.length != dim) {
            throw new IllegalArgumentException("input dimension mismatch, expected " + dim);
        }
        log.debug("Hashing");

        // hashvalues = np.zeros((self.sample_size, 2), dtype=np.int)
        long[][] hashvalues = new long[sampleSize][2];

        // vlog = np.log(v)
        double[] vlog = new double[dim];
        for (int j = 0; j < dim; j++) {
            vlog[j] = log(values[j]);
        }

        // for i in range(self.sample_size):
        for (int i = 0; i < sampleSize; i++) {
            double minLnA = Double.MAX_VALUE;
            int k = 0;
            double minT = 0;

            for (int j = 0; j < dim; j++) {
                // t = np.floor((vlog / self.rs[i]) + self.betas[i])
                double t = floor(vlog[j] / rs[i][j] + betas[i][j]);
                // ln_y = (t - self.betas[i]) * self.rs[i]
                double lnY = (t - betas[i][j]) * rs[i][j];
                // ln_a = self.ln_cs[i] - ln_y - self.rs[i]
                double lnA = (lnCs[i][j] - lnY - rs[i][j]);
                // k = np.nanargmin(ln_a)
                if (!Double.isNaN(lnA) && lnA < minLnA) {
                    minLnA = lnA;
                    k = j;
                    minT = t;
                }
            }

            // hashvalues[i][0], hashvalues[i][1] = k, int(t[k])
            hashvalues[i][0] = k;
            hashvalues[i][1] = (long) minT;
        }

        return hashvalues;
    }
}
