/*
 * =====================================================================================
 *
 *       Filename:  bob_hash.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  05/21/2014 07:55:14 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include <inttypes.h>
#include "bob_hash.h"

#define hashsize(n) ((uint32_t)1<<(n)) 
#define hashmask(n) (hashsize(n)-1)

#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}

/* same, but slower, works on systems that might have 8 byte ub4's */ 
#define mix2(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<< 8); \
  c -= a; c -= b; c ^= ((b&0xffffffff)>>13); \
  a -= b; a -= c; a ^= ((c&0xffffffff)>>12); \
  b -= c; b -= a; b = (b ^ (a<<16)) & 0xffffffff; \
  c -= a; c -= b; c = (c ^ (b>> 5)) & 0xffffffff; \
  a -= b; a -= c; a = (a ^ (c>> 3)) & 0xffffffff; \
  b -= c; b -= a; b = (b ^ (a<<10)) & 0xffffffff; \
  c -= a; c -= b; c = (c ^ (b>>15)) & 0xffffffff; \
}

uint32_t bob_hash(char const *k, int length,uint32_t initval)
{ 
   uint32_t a,b,c,len;

   /* Set up the internal state */ 
   len = length; 
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */ 
   c = initval;           /* the previous hash value */

   /*---------------------------------------- handle most of the key */ 
   while (len >= 3) 
   { 
      a += k[0]; 
      b += k[1]; 
      c += k[2]; 
      mix(a,b,c); 
      k += 3; len -= 3; 
   }

   /*-------------------------------------- handle the last 2 ub4's */ 
   c += (length<<2);   /* <<2 to produce the same results as hash() */ 
   switch(len)              /* all the case statements fall through */ 
   { 
     /* c is reserved for the length */ 
   case 2 : b+=k[1]; 
   case 1 : a+=k[0]; 
     /* case 0: nothing left to add */ 
   } 
   mix(a,b,c); 
   /*-------------------------------------------- report the result */ 
   return c; 
} 
