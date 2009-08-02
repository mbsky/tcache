/*
  Random number generator class
  =============================
  History:

  Created - Sarah "Voodoo Doll" White (2006/01/24)
  Modified - Jason Yap	( 2009/07/21 )
  =============================
  Description:

  This class wraps the Mersenne Twister generator
  with a public interface that supports three common
  pseudorandom number requests:

  === Uniform deviate [0,1) ===
  Random rnd(seed);
  double r = rnd.uniform();

  === Uniform deviate [0,hi) ===
  Random rnd(seed);
  unsigned long r = rnd.uniform(hi);

  === Uniform deviate [lo,hi) ===
  Random rnd(seed);
  unsigned long r = rnd.uniform(lo, hi);

  seed, lo, and hi are user supplied values, with
  seed having a default setting of 1 for debugging
  and testing purposes.
*/

#include "stdafx.h"
#include "LibRandom.h"


double Random::uniform()
{
  return randgen() * (1.0 / (MAX + 1.0));
}

unsigned Random::uniform(unsigned hi)
{
  return static_cast<unsigned>(uniform() * hi);
}

unsigned Random::uniform(unsigned lo, unsigned hi)
{
  return lo + uniform(hi - lo);
}

void Random::seedgen(unsigned long seed)
{
  x[0] = seed & MAX;

  for (int i = 1; i < N; i++) {
    x[i] = (1812433253UL * (x[i - 1] ^ (x[i - 1] >> 30)) + i);
    x[i] &= MAX;
  }
}

// Mersenne Twister algorithm
unsigned long Random::randgen()
{
  unsigned long rnd;

  // Refill the pool when exhausted
  if (next == N) {
    int a;

    for (int i = 0; i < N - 1; i++) {
      rnd = (x[i] & UPPER_MASK) | x[i + 1] & LOWER_MASK;
      a = (rnd & 0x1UL) ? MATRIX_A : 0x0UL;
      x[i] = x[(i + M) % N] ^ (rnd >> 1) ^ a;
    }

    rnd = (x[N - 1] & UPPER_MASK) | x[0] & LOWER_MASK;
    a = (rnd & 0x1UL) ? MATRIX_A : 0x0UL;
    x[N - 1] = x[M - 1] ^ (rnd >> 1) ^ a;

    next = 0; // Rewind index
  }

  rnd = x[next++]; // Grab the next number

  // Voodoo to improve distribution
  rnd ^= (rnd >> 11);
  rnd ^= (rnd << 7) & 0x9d2c5680UL;
  rnd ^= (rnd << 15) & 0xefc60000UL;
  rnd ^= (rnd >> 18);

  return rnd;
}