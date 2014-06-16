#!/usr/bin/env python

import sys
from collections import defaultdict
import codecs
import os
import shutil
from KafNafParserPy import *
from operator import itemgetter
import glob

class My_annot:
    def __init__(self):
        self.wid = ''
        self.token = ''
        self.lemma = ''
        self.pos = ''
        self.opinion_entity = ''
        self.opinion_entity_id = ''
        self.opinion_label = '' ## opinion or nothing
        self.opinion_id = ''
        self.aspect = ''
        self.aspect_id = ''
  
class My_annotations:
  def __init__(self,tag_filename):
    self.annots = []
    self.overall_rating = None
    if os.path.exists(tag_filename):
      fic = codecs.open(tag_filename,'r','utf-8',errors='ignore')
      for line in fic:
          fields = line.split('\t')
          #for n, f in enumerate(fields):
          #    print n,f.encode('utf-8'),' $',
          #print
          anot = My_annot()
          anot.wid = fields[0]
          anot.token = fields[1]
          anot.lemma = fields[2]
          anot.pos = fields[3]
          #fields[4] is nothing
          anot.opinion_entity = fields[5]
          anot.opinion_entity_id = fields[6]
          anot.opinion_label = fields[7]
          anot.opinion_id = fields[8]
          
          ##Changed for attractions, why? No reason, just for make things more difficult
          anot.aspect = fields[9]
          anot.aspect_id = fields[10]
          #print fields
          #print 'aspect', anot.aspect, anot.aspect_id
          ####
          
          if self.overall_rating is None or self.overall_rating == '':
              self.overall_rating = fields[11]
          
          self.annots.append(anot)
      if self.overall_rating == '':
          self.overall_rating = None 
          
  def __iter__(self):
      for anot in self.annots:
          yield anot

          
  def _ss_repr__(self):
      r= ''
      s=set()
      for val in self.annots:
          s.add(val.token)
      return '\n'.join(list(s)).encode('utf-8')
              
      
def check_annotations(tag_filename, kaf_filename, log_notag,log_notag2,log_norate, log_triples,log_aspects,out, log_aspects1, log_potential):
    #print 'Processing',tag_filename
    basefile = os.path.basename(tag_filename)[:-4]
    annotations = My_annotations(tag_filename)
    opinions = {}
    count_types = {}
    tokens_notag2 = []
    str_for_sentence = {}
    sent_for_tokenid = {}
    
    kaf_obj = KafNafParser(kaf_filename)
    for token_obj in kaf_obj.get_tokens():
        token_str = token_obj.get_text()
        token_id = token_obj.get_id()
        token_sent = token_obj.get_sent()
        
        if token_sent not in str_for_sentence:
            str_for_sentence[token_sent]=''
        str_for_sentence[token_sent] += token_str+' '
    
        sent_for_tokenid[token_id] = token_sent
        
   
    aspects = {}
    for anot in annotations:
        wid = anot.wid
        opi_ent = anot.opinion_entity
        opi_ent_id = anot.opinion_entity_id
        op_id = anot.opinion_id
        aspect_label = anot.aspect
        aspect_id = anot.aspect_id
        
        if opi_ent_id != '0' and op_id == '0':
            tokens_notag2.append(wid)

        if op_id is not None and op_id != '0':
            if not op_id in opinions:
                opinions[op_id] = []
            opi_ent = anot.opinion_entity
            opinions[op_id].append((opi_ent,wid,anot.token, opi_ent_id))
        ##########
        
        if aspect_id is not None and aspect_id != '0':
            if aspect_id not in aspects:
                aspects[aspect_id] = []
            aspects[aspect_id].append((aspect_label,wid,anot.token))
    ######
        
    ##Resolve opinions
    for opinion_id, tokens in opinions.items():
        elements = {}
        num_empty = 0
        this_sent = None
        
        previous_type=None
        ents_already_tagged = set()
        crossing_ents = False
        whole_opinion_str = basefile+' Opinion-id:'+opinion_id+'\n'
        for ent, wid, token, ent_id in tokens:
            my_ent = ent
            if my_ent=='': my_ent='none     '
            whole_opinion_str += '\t'+my_ent+'\t'+ent_id+'\t'+token+'\n'
            ##The tokens are in order
            which_type = None
            if ent == 'OpinionTarget': 
                    which_type='target'
            elif ent == 'OpinionHolder': 
                    which_type='holder'
            elif ent!='': 
                    which_type='expression'
            
            ##This will crossing annotations
            if which_type is not None:
                if previous_type == None or which_type != previous_type:
                    if which_type in ents_already_tagged:
                        crossing_ents = True
                    ents_already_tagged.add(which_type)   
                previous_type = which_type
            
            if this_sent is None:
                this_sent = sent_for_tokenid[wid]
                
            if ent_id == '0': 
                num_empty += 1
            if not (ent,ent_id) in elements:
                elements[(ent,ent_id)] = [(wid,token)]
            else:
                elements[(ent,ent_id)].append((wid,token))
                
                
        if num_empty != 0:
            print>>log_notag,basefile,
            print>>log_notag,'\tOpinion id:',opinion_id
            print>>log_notag,'\tNum. of empty tags:',num_empty

        ##Printing to the out file
        print>>out,'Opinion id',opinion_id,' ',str_for_sentence[this_sent].encode('utf-8')
        for (ent, ent_id), eles in elements.items():
            print>>out,'\tType:',ent,' id:'+ent_id,'==>', eles
        print>>out
            
                        
        num_exp = num_tar = num_hol = 0
        for a, b in elements.items():
            if a[0] == 'OpinionTarget': num_tar+=1
            elif a[0] == 'OpinionHolder': num_hol+=1
            elif a[0]!='': num_exp+=1 

        error_triples = False
        if num_exp >=2 and (num_hol>=2 or num_tar>=2):
            error_triples = True
        elif num_hol >= 2 and (num_exp>= 2 or num_tar>=2):
            error_triples = True
        elif num_tar >= 2 and (num_hol>=2 or num_exp>=2):
            error_triples = True
            
        if crossing_ents and error_triples:
            print>>log_potential,whole_opinion_str.encode('utf-8')
            
            
        if error_triples:
            print>>log_triples,basefile,'Opinion-id:',opinion_id
            print>>log_triples,'\tNum expressions: ',num_exp
            print>>log_triples,'\tNum targets:',num_tar
            print>>log_triples,'\tNum holders:',num_hol
     
   
    if annotations.overall_rating is None or annotations.overall_rating == '':
        print>>log_norate,basefile
    
    if len(tokens_notag2) != 0:
        print>>log_notag2,basefile
        print>>log_notag2,'\t',' '.join(tokens_notag2)
        
        
    print>>log_aspects1,basefile
    
    for aspect, tokens in aspects.items():
        wids = ' '.join(wid for _,wid,token in tokens)
        str_tokens =' '.join(token for _,wid,token in tokens)
        type = tokens[0][0]  
        if len(tokens) >= 2:
            print>>log_aspects,basefile
            print>>log_aspects,'\tAspect id',aspect
            print>>log_aspects,'\tType:',type
            print>>log_aspects,'\tTokens:',str_tokens.encode('utf-8')
            print>>log_aspects,'\twids:',wids
            print>>log_aspects
        else:
            print>>log_aspects1,'\t'+type,'-->', str_tokens.encode('utf-8')

    
def check_list_files(tag_folder,kaf_folder, analysis_folder):
    if os.path.exists(analysis_folder):
        shutil.rmtree(analysis_folder)
    os.mkdir(analysis_folder)
    os.mkdir(analysis_folder+'/annotated_opinions')
    
    log_notag = open(analysis_folder+'/log_notags_level1.txt','w')
    log_notag2 = open(analysis_folder+'/log_notags_level2.txt','w')
    log_norate = open(analysis_folder+'/log_norating.txt','w')
    log_aspects= open(analysis_folder+'/log_aspects_span_not_1.txt','w')
    log_aspects1 = open(analysis_folder+'/log_aspects_span_1.txt','w')
    log_triples = open(analysis_folder+'/log_multiple_triples.txt','w')
    log_potential = open(analysis_folder+'/log_crossed_annotations.txt','w')
    log_tag_missing = open(analysis_folder+'/log_tag_missing.txt','w')
    
    num_kaf = 0
    num_tag = 0
    #for tag_file in glob.glob(tag_folder+'/*.tag'):  
    for kaf_file in glob.glob(kaf_folder+'/*.kaf'):
        num_kaf += 1
        basefile = os.path.basename(kaf_file)[:-4]
        tag_file = tag_folder+'/'+basefile+'.tag'
        if os.path.exists(tag_file):
            num_tag += 1
            out_file = analysis_folder+'/annotated_opinions/'+basefile
            out = open(out_file,'w')       
            check_annotations(tag_file ,kaf_file, log_notag, log_notag2, log_norate, log_triples,log_aspects,out,log_aspects1,log_potential)
            out.close()
        else:
            log_tag_missing.write(basefile+'.tag\n')
            
    print 'Processed ', tag_folder
    print '\tNum KAF files:',num_kaf
    print '\tNum TAG files:',num_tag
    log_tag_missing.close()
    log_triples.close()
    #print 'Check',log_triples.name
    log_norate.close()
    #print 'Check', log_norate.name
    log_notag.close()
    #print 'Check', log_notag.name
    log_notag2.close()
    #print 'Check', log_notag2.name
    log_aspects.close()
    #print 'Check',log_aspects.name
    log_aspects1.close()
    log_potential.close()
   

def map_tokens_to_terms(list_tokens,mapping):
    return [mapping.get(token_id) for token_id in list_tokens]


def create_mapping_token_to_term(knaf_obj):
    term_for_token = {}
    for term in knaf_obj.get_terms():
        term_id = term.get_id()
        span_tokens = term.get_span().get_span_ids()
        
        for token_id in span_tokens:
            term_for_token[token_id] = term_id
            
    return term_for_token

    
def extract_opinions_from_file(kaf_filename, tag_filename,out_filename,map_distrib_opi_ents):
    knaf_obj = KafNafParser(kaf_filename)

    ## The opinion layer an properties layers are removed if they exist
    knaf_obj.remove_opinion_layer()
    knaf_obj.remove_properties()


    annotations = My_annotations(tag_filename)

    term_for_token = create_mapping_token_to_term(knaf_obj)
    
    sent_for_wid = {}
    all_sent_ids = set()
    sent_ids_with_opinion = set()
    for token_obj in knaf_obj.get_tokens():
        token_id = token_obj.get_id()
        token_sent = token_obj.get_sent()
        
        all_sent_ids.add(token_sent)
        sent_for_wid[token_id] = token_sent
        
           
    
    opinions = {}
    aspects = {}
    num_triples = 0
    num_scope_opis = 0
    num_skipped = 0 
    num_exps = num_tars = num_hols = 0 
    for anot in annotations:
        wid = anot.wid
        
        opi_ent = anot.opinion_entity
        op_id = anot.opinion_id
        if op_id is not None and op_id != '0':
            if not op_id in opinions:
                opinions[op_id] = []
            opi_ent = anot.opinion_entity
            opinions[op_id].append((anot.opinion_entity,wid,anot.token, anot.opinion_entity_id))
 
        aspect_label = anot.aspect
        aspect_id = anot.aspect_id       
        if aspect_id is not None and aspect_id != '0':
            if aspect_id not in aspects:
                aspects[aspect_id] = []
            aspects[aspect_id].append((aspect_label,wid,anot.token))
    
    
   
    str_opi = tag_filename+' opinion id'
    for opi_id, list_tokens in opinions.items():
        num_scope_opis += 1
        str_opi = '\t'+tag_filename+' opinion id:'+opi_id+'\n'
        elements = {}
        previous_type=None
        ents_already_tagged = set()
        crossing_ents = False
        for ent, wid, token, ent_id in list_tokens:
            
            
            which_type = None
            if ent == 'OpinionTarget': 
                which_type='target'
            elif ent == 'OpinionHolder': 
                which_type='holder'
            elif ent!='': 
                which_type='expression'
            myent= ent
            if ent=='': myent='none    '
            str_opi+='\t\t'+myent+'\t'+token+'\n'
            
            ##This will crossing annotations
            if which_type is not None:
                if previous_type == None or which_type != previous_type:
                    if which_type in ents_already_tagged:
                        crossing_ents = True
                    ents_already_tagged.add(which_type)   
                previous_type = which_type
                
                
            if not (ent,ent_id) in elements:
                elements[(ent,ent_id)] = [(wid,token)]
                #elements[(ent,ent_id)] = [wid]
            else:
                elements[(ent,ent_id)].append((wid,token))
                #elements[(ent,ent_id)].append(wid)

                        
        #print 'Opinion',opi_id,tag_filename
        num_exp = num_tar = num_hol = 0
        targets = []
        holders = []
        expressions = []
        for a, b in elements.items():
            if a[0] == 'OpinionTarget': 
                num_tar+=1
                targets.append(b)
            elif a[0] == 'OpinionHolder': 
                num_hol+=1
                holders.append(b)
            elif a[0]!='': 
                num_exp+=1 
                expressions.append((a[0],b))
        
        opinion_is_ok = True
        if num_exp >=2 and (num_hol>=2 or num_tar>=2):
            opinion_is_ok = False
            #print '\t\tERROR 2 expresssions and 2 targets/holders'
        elif num_hol >= 2 and (num_exp>= 2 or num_tar>=2):
            opinion_is_ok = False
            #print '\t\tERROR 2 holders and 2 exp/targets'
        elif num_tar >= 2 and (num_hol>=2 or num_exp>=2):
            opinion_is_ok = False
            #print '\t\tERROR 2 targets and 2 exp/holders'
        
        
        triples = []
        if len(targets) == 0:
            targets = [[]]
        if len(holders) == 0:
            holders = [[]]
            

        if not opinion_is_ok and crossing_ents:
            print '\tDiscarded ',str_opi.encode('utf-8')
            num_skipped += 1
        else:
            for ent_type, list_wid_token in expressions:
                span_exp = map_tokens_to_terms([wid for wid,token in list_wid_token],term_for_token)
                str_exp = ' '.join(token for wid,token in list_wid_token)
                
                #Add the sentence to opinionated sentence
                for wid,token in list_wid_token:
                    sent_ids_with_opinion.add(sent_for_wid[wid])
                
                for list_wid_token_tar in targets:
                    span_tar = map_tokens_to_terms([wid for wid,token in list_wid_token_tar],term_for_token)
                    str_tar = ' '.join(token for wid,token in list_wid_token_tar)
                    
                    for list_wif_token_hol in holders:
                        span_hol = map_tokens_to_terms([wid for wid,token in list_wif_token_hol],term_for_token)
                        str_hol = ' '.join(token for wid,token in list_wif_token_hol)
                        
                        triples.append((ent_type,span_exp,str_exp,span_tar,str_tar,span_hol,str_hol))
        
        ##Convert triples to opinions
        for n, (ent_type,span_exp,str_exp,span_tar,str_tar,span_hol,str_hol) in enumerate(triples):
            
            holder = Cholder()
            if len(span_hol) != 0:
                span_obj = Cspan()
                span_obj.create_from_ids(span_hol)
                holder.set_span(span_obj)
                holder.set_comment(str_hol)
                num_hols += 1
                map_distrib_opi_ents['holder'][str_hol.lower()]+=1
                
            target = opinion_data.Ctarget()
            if len(span_tar) != 0:
                span_obj = Cspan()
                span_obj.create_from_ids(span_tar)
                target.set_span(span_obj)
                target.set_comment(str_tar)
                num_tars+=1
                map_distrib_opi_ents['target'][str_tar.lower()]+=1
                
            exp = Cexpression()
            span_obj = Cspan()
            span_obj.create_from_ids(span_exp)
            exp.set_span(span_obj)
            exp.set_polarity(ent_type)
            exp.set_comment(str_exp)
            num_exps+=1
            map_distrib_opi_ents['expression'][str_exp.lower()]+=1
            
            my_opinion = Copinion(type='KAF')
            my_opinion.set_holder(holder)
            my_opinion.set_target(target)
            my_opinion.set_expression(exp)
            my_opinion.set_comment('Tag opinion scope id='+str(opi_id))
            my_opinion.set_id('o'+str(num_triples))
            knaf_obj.add_opinion(my_opinion)
            num_triples += 1
            
    ## Add the aspects as features->properties
    # aspects[aspect_id] = [label,wid,token]
    #print aspects
    for aspectid, list_labelwidtoken in aspects.items():
        label = list_labelwidtoken[0]
        term_span = map_tokens_to_terms([wid for label,wid,token in list_labelwidtoken],term_for_token)
        knaf_obj.add_property(label,term_span)
            
    ##Add the linguistic processors
    knaf_obj.add_linguistic_processor('features',Clp(name='Property manual annotations', version='1.0'))
    knaf_obj.add_linguistic_processor('opinions',Clp(name='Manual annotations', version='1.0'))
    knaf_obj.dump(out_filename)
    
    num_sentences = len(all_sent_ids)
    num_sentences_with_opinions = len(sent_ids_with_opinion)
    
    return num_scope_opis, num_skipped, num_triples, num_exps, num_tars, num_hols, num_sentences, num_sentences_with_opinions
                  
        
        
    
def extract_opinions(tag_folder,kaf_folder,out_folder):
    
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)
        
    #For every kaf file in the kaf_folder
    num_kaf_files = num_tag_files = total_scope_opis = total_skipped = total_triples = 0
    total_exps = total_tars = total_hols = 0
    total_sents = total_sents_with_opinions = 0 
    map_distrib_opi_ents = {}
    map_distrib_opi_ents['target']= defaultdict(int)
    map_distrib_opi_ents['holder'] =  defaultdict(int)
    map_distrib_opi_ents['expression'] = defaultdict(int)
    
    #for kaf_filename in glob.glob(kaf_folder+'/*.kaf'):
    for tag_filename in glob.glob(tag_folder+'/*.tag'):
        #num_kaf_files += 1
        num_tag_files += 1
        base_filename = os.path.basename(tag_filename)[:-4]
        out_filename = out_folder+'/'+base_filename+'.kaf'
        print 'Processing ',base_filename
        #tag_filename = tag_folder+'/'+base_filename+'.tag'
        kaf_filename = kaf_folder+'/'+base_filename+'.kaf'
        
        if not os.path.exists(kaf_filename):
            print '\t KAF file not found on ',kaf_filename
        else:
            #num_tag_files += 1
            num_kaf_files += 1
            num_scope_opis, num_skipped, num_triples,  num_exps, num_tars, num_hols, num_sents, num_sents_opis = extract_opinions_from_file(kaf_filename,tag_filename,out_filename,map_distrib_opi_ents)
            total_scope_opis += num_scope_opis
            total_skipped += num_skipped
            total_triples += num_triples
            total_exps += num_exps
            total_tars += num_tars
            total_hols += num_hols
            total_sents += num_sents
            total_sents_with_opinions += num_sents_opis
            print '\tTotal of opinions with different scope in the tag file:',num_scope_opis
            print '\tTotal of skipped opinions:',num_skipped
            print '\tTotal of opinion triples generated:',num_triples
            print '\tTotal of sentences in the tag file',num_sents
            print '\tTotal of sentences with annotated opinions',num_sents_opis
    print 
    print '#'*25
    print 'Total KAF files ', num_kaf_files
    print 'Total TAG files ', num_tag_files
    print 'Total sentences in all TAG files', total_sents
    print 'Total sentences in all TAG files with annotated opinions' , total_sents_with_opinions
    print '% of sentences with opinions: ', round((total_sents_with_opinions*100.0/total_sents),2)
    print 'Opinion density (avg number of opinions per sentence)'
    print '  Considering ALL sentences: %.2f' % (total_scope_opis*1.0/total_sents)
    print '  Considering ANNOTATED sentences: %.2f' % (total_scope_opis*1.0/total_sents_with_opinions)
    print 'Total opinions-scope in tag',total_scope_opis
    print 'Total skipped opinions',total_skipped
    print 'Total generated triples',total_triples
    print 'Total opinion expressions',total_exps,'  Unique: ',len(map_distrib_opi_ents['expression'])
    print 'Total targets non empty', total_tars,'  Unique: ',len(map_distrib_opi_ents['target'])
    print 'Total holders non empty', total_hols,'  Unique: ',len(map_distrib_opi_ents['holder'])
    print
    print 'Most frequent annotations in all the corpus'
    percent = 25
    print '  Expressions'
    v = sorted(map_distrib_opi_ents['expression'].items(),key=itemgetter(1),reverse=True)
    this_max = percent*total_exps/100 ## The first 25%
    for n,(s, freq) in enumerate(v[:this_max]):
        print '    ',s.encode('utf-8'),freq*100.0/total_exps
    
    print '  Targets'
    v = sorted(map_distrib_opi_ents['target'].items(),key=itemgetter(1),reverse=True)
    this_max = percent*total_tars/100 ## The first 25%
    for n,(s, freq) in enumerate(v[:this_max]):
        print '    ',s.encode('utf-8'),freq*100.0/total_tars

    print '  Holders'
    v = sorted(map_distrib_opi_ents['holder'].items(),key=itemgetter(1),reverse=True)
    this_max = 100*total_hols/100 ## The first 25%
    for n,(s, freq) in enumerate(v[:this_max]):
        print '    ',s.encode('utf-8'),freq*100.0/total_hols   

    print '#'*25
    
    

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'Usage: ',sys.argv[0],' -check tag_folder kaf_folder out_folder --> for checking'
        print sys.argv[0],' -generate tag_folder kaf_folder out_folder --> for generating opinions'
        sys.exit(-1)
    if sys.argv[1] == '-check':
        #tag_folder = '/home/izquierdo/data/opener_annotation/hotel_reviews_set1/dutch_hotel_set1_tag'
        tag_folder = sys.argv[2]
        kaf_folder = sys.argv[3]
        out_folder = sys.argv[4]
        check_list_files(tag_folder,kaf_folder,out_folder)
    elif sys.argv[1] == '-generate':
        tag_folder = sys.argv[2] ##'/home/izquierdo/data/opener_annotation/hotel_reviews_set1/dutch_hotel_set1_tag'
        kaf_folder = sys.argv[3] ##'/home/izquierdo/data/opener_annotation/hotel_reviews_set1/dutch_hotel_set1_treetagger_kaf'
        out_folder = sys.argv[4]
        extract_opinions(tag_folder,kaf_folder,out_folder)
    elif sys.argv[1] == '-extract_aspects':
        tag_folder = sys.argv[2]
    else:
        print 'Unknow option',sys.argv[1]
    sys.exit(0)
    

    
